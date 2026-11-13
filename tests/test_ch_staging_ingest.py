"""Tests for loading Companies House bulk CSVs into companies_staging."""

from __future__ import annotations

import csv
import logging
import re
import tempfile
import unittest
from pathlib import Path

import duckdb

from ch_bulk.companies_house.processor import (
    BULK_CSV_COLUMNS,
    _ingest_to_staging,
    process_csvs,
)


def _write_csv(path: Path, rows: list[dict[str, str]]) -> Path:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(BULK_CSV_COLUMNS))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path


def _company(number: str, **overrides: str) -> dict[str, str]:
    row = {
        "CompanyNumber": number,
        "CompanyName": f"Company {number} Ltd",
        "CompanyStatus": "Active",
        "CompanyCategory": "Private Limited Company",
        "IncorporationDate": "01/02/2015",
    }
    row.update(overrides)
    return row


class SicCodeExtractionTests(unittest.TestCase):
    def test_sic_code_is_leading_digits_or_null(self):
        cases = {
            "00000001": ("88100 - Social work activities", "88100"),
            "00000002": ("  01110 - Growing of cereals", "01110"),
            "00000003": ("None Supplied", None),
            "00000004": ("", None),
            "00000005": ("Code 123", None),
        }
        rows = [
            _company(
                number,
                **{
                    "SICCode.SicText_1": text,
                    "SICCode.SicText_2": text,
                    "SICCode.SicText_3": text,
                    "SICCode.SicText_4": text,
                },
            )
            for number, (text, _) in cases.items()
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = _write_csv(Path(tmpdir) / "in.csv", rows)
            con = duckdb.connect()
            try:
                _ingest_to_staging(con, [csv_path])
                got = {
                    number: codes
                    for number, *codes in con.execute(
                        "SELECT company_number, sic_code_1, sic_code_2, "
                        "sic_code_3, sic_code_4 FROM companies_staging"
                    ).fetchall()
                }
            finally:
                con.close()

        for number, (_, expected) in cases.items():
            with self.subTest(number=number):
                self.assertEqual(got[number], [expected] * 4)


class RejectedRowTests(unittest.TestCase):
    def _write_with_malformed_rows(self, path: Path) -> tuple[int, int]:
        """Write good rows interleaved with rows of the wrong column count.

        Returns (good_row_count, malformed_row_count).
        """
        good = [_company(f"{i:08d}") for i in range(1, 21)]
        malformed = [
            "00000099,Too Few Columns Ltd",
            ",".join(["x"] * (len(BULK_CSV_COLUMNS) + 2)),
            "00000098",
        ]
        _write_csv(path, good[:10])
        with path.open("a", newline="", encoding="utf-8") as fh:
            fh.write("".join(line + "\r\n" for line in malformed))
            writer = csv.DictWriter(fh, fieldnames=list(BULK_CSV_COLUMNS))
            writer.writerows(good[10:])
        return len(good), len(malformed)

    def test_ingest_returns_count_of_rejected_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "in.csv"
            good, malformed = self._write_with_malformed_rows(csv_path)
            con = duckdb.connect()
            try:
                rejected = _ingest_to_staging(con, [csv_path])
                loaded = con.execute(
                    "SELECT COUNT(*) FROM companies_staging"
                ).fetchone()[0]
            finally:
                con.close()

        self.assertEqual(loaded, good)
        self.assertEqual(rejected, malformed)

    def test_rejected_count_is_per_ingest_not_cumulative(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bad_path = Path(tmpdir) / "bad.csv"
            _, malformed = self._write_with_malformed_rows(bad_path)
            clean_path = _write_csv(Path(tmpdir) / "clean.csv", [_company("1")])
            con = duckdb.connect()
            try:
                first = _ingest_to_staging(con, [bad_path])
                second = _ingest_to_staging(con, [bad_path])
                third = _ingest_to_staging(con, [clean_path])
            finally:
                con.close()

        self.assertEqual((first, second, third), (malformed, malformed, 0))

    def test_process_csvs_warns_with_rejected_count_and_still_loads(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "BasicCompanyData-2026-05-01-part1_1.csv"
            good, malformed = self._write_with_malformed_rows(csv_path)
            messages: list[str] = []
            with self.assertLogs(
                "ch_bulk.companies_house.processor", level=logging.WARNING
            ) as logs:
                row_count = process_csvs(
                    [csv_path],
                    Path(tmpdir) / "db.duckdb",
                    progress_callback=messages.append,
                    compact=False,
                )

        self.assertEqual(row_count, good)
        warnings = [r for r in logs.records if r.levelno == logging.WARNING]
        count_re = re.compile(rf"\b{malformed}\b.*\brows?\b")
        self.assertTrue(
            any(count_re.search(r.getMessage()) for r in warnings),
            [r.getMessage() for r in warnings],
        )
        self.assertTrue(any(count_re.search(m) for m in messages), messages)


if __name__ == "__main__":
    unittest.main()
