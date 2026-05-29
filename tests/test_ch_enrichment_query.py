"""Tests for CH enrichment browse query helpers."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from ch_bulk.companies_house import query as ch_query
from ch_bulk.companies_house.query import _build_filter_where


class TestFilterWhereAlias(unittest.TestCase):
    def test_default_unqualified(self) -> None:
        where, _params = _build_filter_where(status="active")
        self.assertIn("company_status = ?", where)
        self.assertNotIn("c.company_status", where)

    def test_alias_qualifies(self) -> None:
        where, _params = _build_filter_where(
            status="active",
            postcode_prefix="SW1",
            is_active=True,
            alias="c",
        )
        self.assertIn("c.company_status = ?", where)
        self.assertIn("STARTS_WITH(c.postcode", where)
        self.assertIn("c.is_active = ?", where)


class _ChEnrichmentQueryFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = Path(self.tmpdir.name)
        self.db = self.tmp / "test.duckdb"

        con = duckdb.connect(str(self.db))
        try:
            con.execute(
                """
                CREATE TABLE companies (
                    company_number TEXT,
                    company_name TEXT,
                    company_status TEXT,
                    company_type TEXT,
                    sic_code_1 TEXT,
                    sic_code_2 TEXT,
                    sic_code_3 TEXT,
                    sic_code_4 TEXT,
                    postcode TEXT,
                    incorporation_date DATE,
                    country_of_origin TEXT,
                    is_active BOOLEAN
                )
                """
            )
            con.execute(
                """
                CREATE TABLE company_enrichment (
                    company_number TEXT,
                    avg_director_age INTEGER,
                    min_director_age INTEGER,
                    max_director_age INTEGER,
                    directors_over_60 INTEGER,
                    all_directors_60_plus BOOLEAN,
                    directors_dob_years JSON,
                    revenue DOUBLE,
                    revenue_source TEXT,
                    employee_count INTEGER,
                    filing_period_start DATE,
                    filing_period_end DATE,
                    gross_profit DOUBLE,
                    profit_before_tax DOUBLE,
                    profit_after_tax DOUBLE,
                    fixed_assets DOUBLE,
                    current_assets DOUBLE,
                    total_assets DOUBLE,
                    net_assets DOUBLE,
                    net_current_assets DOUBLE,
                    filing_id TEXT,
                    filing_format TEXT,
                    filing_age_months INTEGER
                )
                """
            )
            con.execute(
                """
                CREATE TABLE ch_cqc_matches (
                    company_number TEXT,
                    cqc_provider_id TEXT,
                    total_score DOUBLE,
                    match_signals TEXT,
                    status TEXT,
                    matched_at TIMESTAMP
                )
                """
            )
            con.execute(
                """
                CREATE TABLE cqc_providers (
                    provider_id TEXT,
                    provider_name TEXT
                )
                """
            )
            con.execute(
                """
                INSERT INTO companies VALUES
                    (
                        'A1', 'Alpha Care Ltd', 'active', 'ltd', '88100', NULL, NULL, NULL,
                        'SW1A 1AA', DATE '2010-01-01', 'United Kingdom', TRUE
                    ),
                    (
                        'A2', 'Beta Support Ltd', 'active', 'ltd', '88100', NULL, NULL, NULL,
                        'SW1A 2BB', DATE '2011-01-01', 'United Kingdom', TRUE
                    ),
                    (
                        'A3', 'Gamma No Dob Ltd', 'active', 'ltd', '88100', NULL, NULL, NULL,
                        'SW1A 3CC', DATE '2012-01-01', 'United Kingdom', TRUE
                    ),
                    (
                        'A4', 'Delta Partial Ltd', 'active', 'ltd', '88100', NULL, NULL, NULL,
                        'SW1A 4DD', DATE '2013-01-01', 'United Kingdom', TRUE
                    ),
                    (
                        'A5', 'Epsilon Metadata Ltd', 'active', 'ltd', '88100', NULL, NULL, NULL,
                        'SW1A 5EE', DATE '2014-01-01', 'United Kingdom', TRUE
                    )
                """
            )
            con.execute(
                """
                INSERT INTO company_enrichment VALUES
                    (
                        'A1', 65, 60, 70, 2, TRUE, '[1960,1956]',
                        1500000.0, 'filed_accounts_ixbrl', 30,
                        DATE '2025-04-01', DATE '2026-03-31',
                        400000.0, 200000.0, 150000.0,
                        100000.0, 500000.0, 600000.0, 350000.0, 300000.0,
                        'file-a1', 'ixbrl', 2
                    ),
                    (
                        'A2', 40, 35, 45, 0, FALSE, '[1986,1981]',
                        NULL, NULL, NULL,
                        NULL, NULL,
                        NULL, NULL, NULL,
                        NULL, NULL, NULL, NULL, NULL,
                        NULL, NULL, NULL
                    ),
                    (
                        'A3', NULL, NULL, NULL, 0, FALSE, '[]',
                        300000.0, 'employee_band_lookup', 8,
                        DATE '2025-04-01', DATE '2026-03-31',
                        120000.0, 60000.0, 45000.0,
                        50000.0, 150000.0, 200000.0, 140000.0, 80000.0,
                        'file-a3', 'pdf', 3
                    ),
                    (
                        'A4', 52, 48, 59, 0, FALSE, '[1978,1974]',
                        NULL, 'partial_no_revenue', 27,
                        DATE '2025-03-01', DATE '2026-02-28',
                        NULL, NULL, NULL,
                        1705232.0, 953243.0, 2658475.0, 1382731.0, 640538.0,
                        'file-a4', 'ixbrl', 1
                    ),
                    (
                        'A5', 48, 44, 53, 0, FALSE, '[1982,1977]',
                        NULL, 'pdf_no_text_layer', NULL,
                        NULL, NULL,
                        NULL, NULL, NULL,
                        NULL, NULL, NULL, NULL, NULL,
                        'file-a5', 'pdf', 1
                    )
                """
            )
            con.execute(
                """
                INSERT INTO ch_cqc_matches VALUES
                    ('A1', 'P1', 0.95, 'name+postcode', 'user_confirmed', TIMESTAMP '2026-05-29 12:00:00'),
                    ('A3', 'P3', 0.72, 'name', 'auto_confirmed', TIMESTAMP '2026-05-29 12:05:00')
                """
            )
            con.execute(
                """
                INSERT INTO cqc_providers VALUES
                    ('P1', 'Acme Group'),
                    ('P3', 'Gamma Care Network')
                """
            )
            con.execute(
                """
                CREATE VIEW current_company_match AS
                WITH ranked AS (
                    SELECT
                        company_number,
                        cqc_provider_id,
                        total_score,
                        match_signals,
                        status,
                        matched_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY company_number
                            ORDER BY matched_at DESC, cqc_provider_id ASC
                        ) AS rn
                    FROM ch_cqc_matches
                    WHERE status IN ('user_confirmed', 'auto_confirmed', 'needs_review')
                )
                SELECT
                    company_number,
                    cqc_provider_id,
                    total_score,
                    match_signals,
                    status,
                    matched_at
                FROM ranked
                WHERE rn = 1
                """
            )
        finally:
            con.close()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()


class TestDirectors(_ChEnrichmentQueryFixture):
    def test_directors_excludes_null_age(self) -> None:
        rows, total = ch_query.query_directors_age(self.db)
        self.assertEqual({row["company_number"] for row in rows}, {"A1", "A2", "A4", "A5"})
        self.assertEqual(total, 4)

    def test_min_director_age_any_director(self) -> None:
        rows, _total = ch_query.query_directors_age(self.db, min_director_age=60)
        self.assertEqual({row["company_number"] for row in rows}, {"A1"})

    def test_search_matches_provider_name(self) -> None:
        rows, _total = ch_query.query_directors_age(self.db, search="acme group")
        self.assertEqual({row["company_number"] for row in rows}, {"A1"})

    def test_invalid_sort_by_falls_back_to_company_name(self) -> None:
        rows, _total = ch_query.query_directors_age(self.db, sort_by="not_a_column")
        self.assertEqual([row["company_number"] for row in rows], ["A1", "A2", "A4", "A5"])

    def test_missing_enrichment_tables_return_empty(self) -> None:
        empty_db = self.tmp / "empty.duckdb"
        con = duckdb.connect(str(empty_db))
        try:
            con.execute(
                """
                CREATE TABLE companies (
                    company_number TEXT,
                    company_name TEXT,
                    company_status TEXT,
                    company_type TEXT,
                    sic_code_1 TEXT,
                    sic_code_2 TEXT,
                    sic_code_3 TEXT,
                    sic_code_4 TEXT,
                    postcode TEXT,
                    incorporation_date DATE,
                    country_of_origin TEXT,
                    is_active BOOLEAN
                )
                """
            )
            con.execute(
                """
                INSERT INTO companies VALUES
                    (
                        'Z1', 'Zeta Ltd', 'active', 'ltd', '88100', NULL, NULL, NULL,
                        'SW1A 9ZZ', DATE '2020-01-01', 'United Kingdom', TRUE
                    )
                """
            )
        finally:
            con.close()

        self.assertEqual(ch_query.query_directors_age(empty_db), ([], 0))


class TestFinancials(_ChEnrichmentQueryFixture):
    def test_financials_excludes_no_data(self) -> None:
        rows, total = ch_query.query_financials(self.db)
        company_numbers = {row["company_number"] for row in rows}
        self.assertEqual(company_numbers, {"A1", "A3", "A4"})
        self.assertEqual(total, 3)

    def test_min_revenue(self) -> None:
        rows, _total = ch_query.query_financials(self.db, min_revenue=1_000_000)
        self.assertEqual({row["company_number"] for row in rows}, {"A1"})
        self.assertTrue(all(row["revenue"] >= 1_000_000 for row in rows))

    def test_min_employees(self) -> None:
        rows, _total = ch_query.query_financials(self.db, min_employees=10)
        self.assertEqual({row["company_number"] for row in rows}, {"A1", "A4"})
        self.assertTrue(all((row["employee_count"] or 0) >= 10 for row in rows))


class TestCompaniesSearch(_ChEnrichmentQueryFixture):
    def test_companies_search_blank_uses_bare_path(self) -> None:
        plain_db = self.tmp / "plain.duckdb"
        con = duckdb.connect(str(plain_db))
        try:
            con.execute(
                """
                CREATE TABLE companies (
                    company_number TEXT,
                    company_name TEXT,
                    company_status TEXT,
                    company_type TEXT,
                    sic_code_1 TEXT,
                    sic_code_2 TEXT,
                    sic_code_3 TEXT,
                    sic_code_4 TEXT,
                    postcode TEXT,
                    incorporation_date DATE,
                    country_of_origin TEXT,
                    is_active BOOLEAN
                )
                """
            )
            con.execute(
                """
                INSERT INTO companies VALUES
                    (
                        'B1', 'Bare Path Ltd', 'active', 'ltd', '88100', NULL, NULL, NULL,
                        'SW1A 7BP', DATE '2019-01-01', 'United Kingdom', TRUE
                    )
                """
            )
        finally:
            con.close()

        rows, total = ch_query.query_companies(plain_db)
        self.assertEqual(total, 1)
        self.assertEqual({row["company_number"] for row in rows}, {"B1"})

    def test_companies_search_provider(self) -> None:
        rows, _total = ch_query.query_companies(self.db, search="acme group")
        self.assertEqual({row["company_number"] for row in rows}, {"A1"})


class TestExports(_ChEnrichmentQueryFixture):
    def test_export_directors(self) -> None:
        out = self.tmp / "directors.csv"
        count = ch_query.export_directors_age_csv(
            self.db,
            out,
            min_director_age=60,
        )
        self.assertEqual(count, 1)
        self.assertIn("provider_name", out.read_text().splitlines()[0])

    def test_export_financials(self) -> None:
        out = self.tmp / "financials.csv"
        count = ch_query.export_financials_csv(
            self.db,
            out,
            min_employees=10,
        )
        self.assertEqual(count, 2)
        self.assertIn("current_assets", out.read_text().splitlines()[0])

    def test_export_companies_search_provider(self) -> None:
        out = self.tmp / "companies.csv"
        count = ch_query.export_filtered_csv(self.db, out, search="acme group")
        self.assertEqual(count, 1)
        header = out.read_text().splitlines()[0].split(",")
        self.assertEqual(header.count("company_number"), 1)


class TestApiChEnrichment(_ChEnrichmentQueryFixture):
    def test_api_directors(self) -> None:
        from ch_bulk import ChBulk

        temp_data_dir = self.tmp / "data"
        temp_data_dir.mkdir()
        ch = ChBulk(data_dir=temp_data_dir, db_path=self.db)
        rows, total = ch.query_directors_age_advanced(min_director_age=60)
        self.assertEqual(total, 1)
        self.assertEqual({row["company_number"] for row in rows}, {"A1"})

    def test_api_financials_export(self) -> None:
        from ch_bulk import ChBulk

        temp_data_dir = self.tmp / "data-export"
        temp_data_dir.mkdir()
        ch = ChBulk(data_dir=temp_data_dir, db_path=self.db)
        out = self.tmp / "api-financials.csv"
        count = ch.export_financials_csv(out, min_employees=10)
        self.assertEqual(count, 2)
        self.assertTrue(out.exists())

    def test_api_companies_search_passthrough(self) -> None:
        from ch_bulk import ChBulk

        temp_data_dir = self.tmp / "data-search"
        temp_data_dir.mkdir()
        ch = ChBulk(data_dir=temp_data_dir, db_path=self.db)
        rows, total = ch.query_advanced(search="acme group")
        self.assertEqual(total, 1)
        self.assertEqual({row["company_number"] for row in rows}, {"A1"})


if __name__ == "__main__":
    unittest.main()
