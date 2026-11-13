"""Regression tests for AUDIT-02, AUDIT-03, and AUDIT-04.

Empty SIC input, negative pagination/limit values, and blank company IDs
must fail with ValueError (or be skipped) instead of producing invalid
SQL, DuckDB binder errors, or a phantom 00000000 company number.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from ch_bulk.companies_house.financials_fetch import _normalize_company_ids
from ch_bulk.companies_house.query import (
    _build_sic_where,
    export_query_csv,
    query_by_sic,
    query_companies,
    query_directors_age,
    query_financials,
)
from ch_bulk.cqc.query import (
    query_cqc_locations,
    query_cqc_providers,
    query_hsca_locations,
)
from ch_bulk.matching.query import query_tiered_targets


def _make_companies_db(path: Path) -> Path:
    con = duckdb.connect(str(path))
    try:
        con.execute(
            """
            CREATE TABLE companies (
                company_number VARCHAR,
                company_name VARCHAR,
                company_status VARCHAR,
                company_type VARCHAR,
                postcode VARCHAR,
                incorporation_date DATE,
                country_of_origin VARCHAR,
                is_active BOOLEAN,
                sic_code_1 VARCHAR,
                sic_code_2 VARCHAR,
                sic_code_3 VARCHAR,
                sic_code_4 VARCHAR
            )
            """
        )
        con.execute(
            "INSERT INTO companies VALUES "
            "('12345678', 'ACME LTD', 'Active', 'ltd', 'SW1A 1AA', "
            "'2020-01-01', 'United Kingdom', TRUE, '88100', NULL, NULL, NULL)"
        )
    finally:
        con.close()
    return path


def _empty_db(path: Path) -> Path:
    con = duckdb.connect(str(path))
    con.close()
    return path


class EmptySicTests(unittest.TestCase):
    def test_build_sic_where_rejects_empty(self) -> None:
        with self.assertRaises(ValueError):
            _build_sic_where([])

    def test_query_by_sic_rejects_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _make_companies_db(Path(tmpdir) / "test.duckdb")
            for bad in ("", [], ["", "  "]):
                with self.subTest(bad=bad), self.assertRaises(ValueError):
                    query_by_sic(db_path, bad)

    def test_query_by_sic_with_single_code_still_works(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _make_companies_db(Path(tmpdir) / "test.duckdb")
            self.assertEqual(len(query_by_sic(db_path, "88100")), 1)

    def test_query_companies_rejects_empty_sic_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _make_companies_db(Path(tmpdir) / "test.duckdb")
            for bad in ("", []):
                with self.subTest(bad=bad), self.assertRaises(ValueError):
                    query_companies(db_path, sic_codes=bad)

    def test_query_companies_without_sic_filter_still_works(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _make_companies_db(Path(tmpdir) / "test.duckdb")
            rows, total = query_companies(db_path, sic_codes=None)
            self.assertEqual((total, len(rows)), (1, 1))


class PaginationValidationTests(unittest.TestCase):
    def test_negative_limit_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _make_companies_db(Path(tmpdir) / "test.duckdb")
            with self.assertRaises(ValueError):
                query_by_sic(db_path, "88100", limit=-1)
            with self.assertRaises(ValueError):
                export_query_csv(
                    db_path, "88100", Path(tmpdir) / "out.csv", limit=-1
                )

    def test_zero_limit_returns_no_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _make_companies_db(Path(tmpdir) / "test.duckdb")
            self.assertEqual(query_by_sic(db_path, "88100", limit=0), [])

    def test_invalid_page_rejected_by_every_paginated_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _empty_db(Path(tmpdir) / "test.duckdb")
            callers = [
                lambda page, size: query_companies(db_path, page=page, page_size=size),
                lambda page, size: query_directors_age(db_path, page=page, page_size=size),
                lambda page, size: query_financials(db_path, page=page, page_size=size),
                lambda page, size: query_cqc_locations(db_path, page=page, page_size=size),
                lambda page, size: query_cqc_providers(db_path, page=page, page_size=size),
                lambda page, size: query_hsca_locations(db_path, page=page, page_size=size),
                lambda page, size: query_tiered_targets(db_path, page=page, page_size=size),
            ]
            for caller in callers:
                for page, size in ((0, 50), (-1, 50), (1, 0), (1, -5)):
                    with self.subTest(page=page, size=size), self.assertRaises(ValueError):
                        caller(page, size)


class NormalizeCompanyIdsTests(unittest.TestCase):
    def test_blank_ids_are_skipped_before_padding(self) -> None:
        self.assertEqual(_normalize_company_ids(["", "  ", "123"]), ["00000123"])
        self.assertEqual(_normalize_company_ids([""]), [])
        self.assertEqual(_normalize_company_ids([]), [])

    def test_valid_ids_still_padded_and_deduped(self) -> None:
        self.assertEqual(
            _normalize_company_ids(["1234567", "01234567", "1234567"]),
            ["01234567"],
        )


if __name__ == "__main__":
    unittest.main()
