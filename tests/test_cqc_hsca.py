"""Tests for HSCA download + ODS ingest."""

from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import duckdb

from ch_bulk.cqc_downloader import download_hsca_filters
from ch_bulk.cqc_processor import process_hsca_filters
from ch_bulk.processor import SanityCheckError


class _MockResponse:
    def __init__(self, text: str = "") -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


class _MockStreamResponse:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks
        self.headers = {"Content-Length": str(sum(len(chunk) for chunk in chunks))}

    def __enter__(self) -> "_MockStreamResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def raise_for_status(self) -> None:
        return None

    def iter_bytes(self, chunk_size: int = 0):
        del chunk_size
        yield from self._chunks


def _write_ods_fixture(
    path: Path,
    *,
    hsca_rows: list[dict[str, str]],
    dual_rows: list[dict[str, str]],
) -> None:
    from odf.opendocument import OpenDocumentSpreadsheet
    from odf.table import Table, TableCell, TableRow
    from odf.text import P

    def add_sheet(
        doc: OpenDocumentSpreadsheet,
        name: str,
        rows: list[dict[str, str]],
    ) -> None:
        table = Table(name=name)
        if rows:
            headers = list(rows[0].keys())
            header_row = TableRow()
            for header in headers:
                cell = TableCell(valuetype="string")
                cell.addElement(P(text=header))
                header_row.addElement(cell)
            table.addElement(header_row)

            for row in rows:
                table_row = TableRow()
                for header in headers:
                    cell = TableCell(valuetype="string")
                    cell.addElement(P(text=row.get(header, "")))
                    table_row.addElement(cell)
                table.addElement(table_row)
        doc.spreadsheet.addElement(table)

    doc = OpenDocumentSpreadsheet()
    add_sheet(doc, "README", [{"Notes": "fixture"}])
    add_sheet(doc, "HSCA_Active_Locations", hsca_rows)
    add_sheet(doc, "Dual_Registration_Locations", dual_rows)
    doc.save(str(path), addsuffix=False)


class HSCADownloaderTests(unittest.TestCase):
    def test_download_hsca_filters_fetches_latest_matching_ods(self):
        listing_html = """
        <a href="/sites/default/files/2026-04/20_April_2026_HSCA_Active_Locations.ods">old</a>
        <a href="/sites/default/files/2026-05/05_May_2026_HSCA_Active_Locations.ods">new</a>
        """

        with tempfile.TemporaryDirectory() as tmpdir:
            with (
                patch(
                    "ch_bulk.cqc_downloader.httpx.get",
                    return_value=_MockResponse(listing_html),
                ),
                patch(
                    "ch_bulk.cqc_downloader.httpx.stream",
                    return_value=_MockStreamResponse([b"chunk-a", b"chunk-b"]),
                ),
            ):
                path = download_hsca_filters(Path(tmpdir))

            self.assertEqual(path.name, "hsca_active_locations_2026-05-05.ods")
            self.assertEqual(path.read_bytes(), b"chunk-achunk-b")


class HSCAProcessorTests(unittest.TestCase):
    def _build_hsca_rows(self, with_company_numbers: bool = True) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        for idx in range(5):
            rows.append(
                {
                    "Location ID": f"loc-{idx}",
                    "Provider ID": f"prov-{idx}",
                    "Provider Companies House Number": (
                        f"0000000{idx}" if with_company_numbers and idx < 3 else ""
                    ),
                    "Provider Charity Number": "",
                    "Provider Ownership Type": "Organisation",
                    "Brand ID": f"brand-{idx}",
                    "Brand Name": f"Brand {idx}",
                    "Provider Web Address": f"https://provider-{idx}.example",
                    "Location Web Address": f"https://location-{idx}.example",
                    "Care home?": "Y" if idx % 2 == 0 else "N",
                    "Care homes beds": str(10 + idx),
                    "Dormant (Y/N)": "N",
                    "Registered manager": f"Manager {idx}",
                    "Service type - Domiciliary care service": "Y" if idx != 4 else "N",
                    "Service type - Supported living service": "Y" if idx == 4 else "N",
                    "Service type - Care home service with nursing": "N",
                    "Service type - Care home service without nursing": "Y" if idx == 0 else "N",
                    "Service type - Extra Care housing services": "N",
                    "Service type - Hospice services at home": "N",
                    "Some Unmapped Column": f"raw-{idx}",
                }
            )
        return rows

    def _build_dual_rows(self) -> list[dict[str, str]]:
        return [
            {
                "Location ID": "loc-0",
                "Location Name": "Location 0",
                "Location HSCA Start Date": "05/05/2026",
                "Location Type/Sector": "Social Care Org",
                "Provider ID": "prov-0",
                "Provider Name": "Provider 0",
                "Linked Organisation ID": "linked-0",
                "Linked Organisation Name": "Linked Org 0",
                "Relationship": "Dual Registration",
                "Relationship Start Date": "05/05/2026",
                "Primary ID": "Y",
            },
            {
                "Location ID": "loc-1",
                "Location Name": "Location 1",
                "Location HSCA Start Date": "05/05/2026",
                "Location Type/Sector": "Social Care Org",
                "Provider ID": "prov-1",
                "Provider Name": "Provider 1",
                "Linked Organisation ID": "linked-1",
                "Linked Organisation Name": "Linked Org 1",
                "Relationship": "Dual Registration",
                "Relationship Start Date": "05/05/2026",
                "Primary ID": "N",
            },
        ]

    def test_process_hsca_filters_parses_fixture_and_tracks_batch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            db_path = tmp_path / "test.duckdb"
            ods_path = tmp_path / "hsca_active_locations_2026-05-05.ods"
            _write_ods_fixture(
                ods_path,
                hsca_rows=self._build_hsca_rows(with_company_numbers=True),
                dual_rows=self._build_dual_rows(),
            )

            row_count = process_hsca_filters(
                ods_path,
                db_path,
                compact=False,
                scrape_date_override=date(2026, 5, 5),
            )
            self.assertEqual(row_count, 5)

            con = duckdb.connect(str(db_path))
            try:
                locations = con.execute(
                    """
                    SELECT
                        provider_companies_house_number,
                        st_domiciliary_care_service,
                        st_supported_living_service,
                        raw_row
                    FROM cqc_hsca_locations
                    WHERE location_id = 'loc-4'
                    """
                ).fetchone()
                self.assertEqual(locations[0], None)
                self.assertEqual(locations[1], False)
                self.assertEqual(locations[2], True)
                self.assertIn('"Some Unmapped Column": "raw-4"', locations[3])

                dual = con.execute(
                    """
                    SELECT primary_id
                    FROM cqc_hsca_dual_registrations
                    WHERE location_id = 'loc-0'
                    """
                ).fetchone()
                self.assertEqual(dual[0], True)

                batch = con.execute(
                    """
                    SELECT sync_type, status, records_fetched, records_updated, error_count
                    FROM cqc_sync_batches
                    ORDER BY started_at DESC
                    LIMIT 1
                    """
                ).fetchone()
                self.assertEqual(batch, ("bulk_hsca", "succeeded", 5, 5, 0))
            finally:
                con.close()

            updated_rows = self._build_hsca_rows(with_company_numbers=True)[:3]
            _write_ods_fixture(
                ods_path,
                hsca_rows=updated_rows,
                dual_rows=self._build_dual_rows()[:1],
            )
            row_count = process_hsca_filters(
                ods_path,
                db_path,
                compact=False,
                scrape_date_override=date(2026, 5, 5),
                force=True,
            )
            self.assertEqual(row_count, 3)

            con = duckdb.connect(str(db_path))
            try:
                self.assertEqual(
                    con.execute("SELECT COUNT(*) FROM cqc_hsca_locations").fetchone()[0],
                    3,
                )
                self.assertEqual(
                    con.execute(
                        "SELECT COUNT(*) FROM cqc_hsca_dual_registrations"
                    ).fetchone()[0],
                    1,
                )
            finally:
                con.close()

    def test_process_hsca_filters_rejects_low_company_number_population(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            db_path = tmp_path / "test.duckdb"
            ods_path = tmp_path / "hsca_active_locations_2026-05-05.ods"
            _write_ods_fixture(
                ods_path,
                hsca_rows=self._build_hsca_rows(with_company_numbers=False),
                dual_rows=self._build_dual_rows(),
            )

            with self.assertRaises(SanityCheckError):
                process_hsca_filters(
                    ods_path,
                    db_path,
                    compact=False,
                    scrape_date_override=date(2026, 5, 5),
                )

            con = duckdb.connect(str(db_path))
            try:
                batch = con.execute(
                    """
                    SELECT sync_type, status, error_count
                    FROM cqc_sync_batches
                    ORDER BY started_at DESC
                    LIMIT 1
                    """
                ).fetchone()
                self.assertEqual(batch, ("bulk_hsca", "failed", 1))
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
