"""Companies House bulk downloader: reuse of already-extracted parts."""

from __future__ import annotations

import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import httpx

from ch_bulk.companies_house import downloader
from ch_bulk.companies_house.downloader import download_bulk_data
from ch_bulk.core.paths import ch_input_dir

MONTH = "2026-05"


def _csv_name(part: int) -> str:
    return f"BasicCompanyData-{MONTH}-01-part{part}_7.csv"


def _fake_extract(zip_path: Path, extract_dir: Path) -> list[Path]:
    csv_path = extract_dir / zip_path.name.replace(".zip", ".csv")
    csv_path.write_text("CompanyName\n", encoding="utf-8")
    return [csv_path]


class ExistingPartsTests(unittest.TestCase):
    def test_all_parts_extracted_downloads_nothing(self):
        messages: list[str] = []
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = ch_input_dir(tmpdir)
            input_dir.mkdir(parents=True)
            for part in range(1, 8):
                (input_dir / _csv_name(part)).write_text("x\n", encoding="utf-8")

            with patch.object(downloader, "_download_file") as download_file, patch.object(
                downloader, "_extract_zip"
            ) as extract_zip:
                result = download_bulk_data(
                    data_dir=tmpdir,
                    month=MONTH,
                    progress_callback=messages.append,
                )

            download_file.assert_not_called()
            extract_zip.assert_not_called()
            self.assertEqual(result, [input_dir / _csv_name(p) for p in range(1, 8)])
            self.assertTrue(any("already" in m.lower() for m in messages))

    def test_only_missing_parts_are_downloaded(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = ch_input_dir(tmpdir)
            input_dir.mkdir(parents=True)
            for part in (1, 2, 3):
                (input_dir / _csv_name(part)).write_text("x\n", encoding="utf-8")

            with patch.object(downloader, "_download_file") as download_file, patch.object(
                downloader, "_extract_zip", side_effect=_fake_extract
            ):
                result = download_bulk_data(
                    data_dir=tmpdir,
                    month=MONTH,
                    progress_callback=lambda _msg: None,
                )

            downloaded = [call.args[2].name for call in download_file.call_args_list]
            self.assertEqual(
                downloaded,
                [f"BasicCompanyData-{MONTH}-01-part{p}_7.zip" for p in (4, 5, 6, 7)],
            )
            self.assertEqual(
                sorted(result), sorted(input_dir / _csv_name(p) for p in range(1, 8))
            )

    def test_cli_path_also_skips_extracted_parts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = ch_input_dir(tmpdir)
            input_dir.mkdir(parents=True)
            for part in range(1, 8):
                (input_dir / _csv_name(part)).write_text("x\n", encoding="utf-8")

            with patch.object(downloader, "_download_file") as download_file:
                download_bulk_data(data_dir=tmpdir, month=MONTH)

            download_file.assert_not_called()


class ExtractionIsAtomicTests(unittest.TestCase):
    """A part counts as done when its CSV exists, so a CSV must never be
    left half-written by an interrupted extraction."""

    def _make_zip(self, tmpdir: str) -> Path:
        zip_path = Path(tmpdir) / f"BasicCompanyData-{MONTH}-01-part1_7.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr(_csv_name(1), "CompanyName\nACME\n" * 1000)
        return zip_path

    def test_extract_writes_csv_and_leaves_no_temp_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = self._make_zip(tmpdir)
            out_dir = Path(tmpdir) / "out"
            out_dir.mkdir()

            result = downloader._extract_zip(zip_path, out_dir)

            self.assertEqual(result, [out_dir / _csv_name(1)])
            self.assertEqual(
                (out_dir / _csv_name(1)).read_text(encoding="utf-8"),
                "CompanyName\nACME\n" * 1000,
            )
            self.assertEqual(sorted(p.name for p in out_dir.iterdir()), [_csv_name(1)])

    def test_interrupted_extract_leaves_no_csv(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = self._make_zip(tmpdir)
            out_dir = Path(tmpdir) / "out"
            out_dir.mkdir()

            with patch.object(
                downloader.shutil, "copyfileobj", side_effect=OSError("disk full")
            ):
                with self.assertRaises(OSError):
                    downloader._extract_zip(zip_path, out_dir)

            self.assertFalse((out_dir / _csv_name(1)).exists())


def _server(full: bytes, *, truncate_first_get_to: int | None = None):
    """A fake download server that honours HEAD and Range requests.

    The first GET can be cut short while still advertising the full length,
    which is what a dropped connection looks like.
    """
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "HEAD":
            return httpx.Response(200, headers={"content-length": str(len(full))})
        range_header = request.headers.get("Range")
        start = int(range_header.split("=")[1].rstrip("-")) if range_header else 0
        body = full[start:]
        status = 206 if range_header else 200
        gets = [r for r in requests if r.method == "GET"]
        sent = body
        if truncate_first_get_to is not None and len(gets) == 1:
            sent = body[:truncate_first_get_to]
        return httpx.Response(
            status, headers={"content-length": str(len(body))}, content=sent
        )

    return httpx.Client(transport=httpx.MockTransport(handler)), requests


class IncompleteDownloadTests(unittest.TestCase):
    URL = "https://download.example/part1.zip"

    def test_short_download_is_an_error(self):
        client, _ = _server(b"x" * 100, truncate_first_get_to=40)
        with tempfile.TemporaryDirectory() as tmpdir, client:
            dest = Path(tmpdir) / "part1.zip"
            with self.assertRaises(downloader.IncompleteDownloadError):
                downloader._download_file(client, self.URL, dest)

    def test_short_download_is_retried_and_resumed(self):
        full = bytes(range(100))
        client, requests = _server(full, truncate_first_get_to=40)
        messages: list[str] = []
        # download_bulk_data opens (and closes) the client itself.
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(downloader.httpx, "Client", return_value=client), patch.object(
                downloader, "_extract_zip", side_effect=_fake_extract
            ), patch.object(downloader.time, "sleep"):
                download_bulk_data(
                    data_dir=tmpdir, month=MONTH, keep_zips=True,
                    progress_callback=messages.append,
                )
            part1 = ch_input_dir(tmpdir) / f"BasicCompanyData-{MONTH}-01-part1_7.zip"
            self.assertEqual(part1.read_bytes(), full)
        resumed = [r for r in requests if r.headers.get("Range")]
        self.assertEqual([r.headers["Range"] for r in resumed], ["bytes=40-"])

    def test_cli_and_gui_paths_retry_the_same_way(self):
        for callback in (None, lambda _msg: None):
            with self.subTest(gui=callback is not None), tempfile.TemporaryDirectory() as tmpdir:
                failures = [downloader.IncompleteDownloadError("short"), httpx.ConnectError("x")]

                def flaky(*_args, **_kwargs):
                    if failures:
                        raise failures.pop(0)

                with patch.object(downloader, "_download_file", side_effect=flaky) as dl, \
                        patch.object(downloader, "_extract_zip", side_effect=_fake_extract), \
                        patch.object(downloader.time, "sleep"):
                    result = download_bulk_data(
                        data_dir=tmpdir, month=MONTH, progress_callback=callback,
                    )
                self.assertEqual(len(result), 7)
                self.assertEqual(dl.call_count, 9)

    def test_gives_up_after_the_last_attempt(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(
                downloader, "_download_file",
                side_effect=downloader.IncompleteDownloadError("short"),
            ), patch.object(downloader.time, "sleep"):
                with self.assertRaises(downloader.IncompleteDownloadError):
                    download_bulk_data(data_dir=tmpdir, month=MONTH)


if __name__ == "__main__":
    unittest.main()
