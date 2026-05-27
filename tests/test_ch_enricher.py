"""Focused tests for CH director-age and revenue enrichment logic."""

from __future__ import annotations

import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

import duckdb

from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.companies_house.ch_enricher import compute_age_fields, enrich_directors, enrich_revenue
from ch_bulk.companies_house.revenue_model import estimate, load_bands


class AgeFieldTests(unittest.TestCase):
    def test_compute_age_fields_uses_active_directors_only(self):
        officers = [
            {
                "officer_role": "director",
                "date_of_birth": {"year": 1960},
            },
            {
                "officer_role": "director",
                "date_of_birth": {"year": 1970},
            },
            {
                "officer_role": "director",
                "date_of_birth": {"year": 1950},
                "resigned_on": "2020-01-01",
            },
            {
                "officer_role": "secretary",
                "date_of_birth": {"year": 1955},
            },
        ]
        result = compute_age_fields(officers, current_year=2026)
        self.assertEqual(result["avg_director_age"], 61)
        self.assertEqual(result["min_director_age"], 56)
        self.assertEqual(result["max_director_age"], 66)
        self.assertEqual(result["directors_over_60"], 1)
        self.assertEqual(result["all_directors_60_plus"], False)
        self.assertEqual(result["directors_dob_years"], [1960, 1970])

    def test_compute_age_fields_handles_no_active_directors(self):
        officers = [
            {
                "officer_role": "director",
                "date_of_birth": {"year": 1960},
                "resigned_on": "2020-01-01",
            }
        ]
        result = compute_age_fields(officers, current_year=2026)
        self.assertEqual(result["avg_director_age"], None)
        self.assertEqual(result["directors_over_60"], 0)
        self.assertEqual(result["all_directors_60_plus"], False)
        self.assertEqual(result["directors_dob_years"], [])


class RevenueModelTests(unittest.TestCase):
    def test_load_bands_and_estimate(self):
        bands_path = (
            Path(__file__).resolve().parent.parent
            / "data"
            / "reference"
            / "revenue_bands.csv"
        )
        bands = load_bands(bands_path)
        self.assertGreaterEqual(len(bands), 5)
        self.assertEqual(estimate(75, bands), 3000000.0)
        self.assertIsNone(estimate(0, bands))

    def test_enrich_revenue_uses_employee_count_and_preserves_nulls(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "revenue.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute(
                    """
                    CREATE TABLE companies (
                        company_number TEXT PRIMARY KEY,
                        company_name TEXT,
                        postcode TEXT,
                        address_post_town TEXT,
                        sic_code_1 TEXT,
                        sic_code_2 TEXT,
                        sic_code_3 TEXT,
                        sic_code_4 TEXT
                    )
                    """
                )
                ensure_pipeline_schema(con)
                con.execute(
                    """
                    INSERT INTO companies VALUES
                        ('11111111', 'Alpha', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL),
                        ('22222222', 'Beta', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL)
                    """
                )
                con.execute(
                    """
                    INSERT INTO company_enrichment (
                        company_number,
                        avg_director_age,
                        min_director_age,
                        max_director_age,
                        directors_over_60,
                        all_directors_60_plus,
                        directors_dob_years,
                        revenue,
                        revenue_source,
                        employee_count,
                        filing_period_start,
                        filing_period_end,
                        gross_profit,
                        profit_before_tax,
                        profit_after_tax,
                        fixed_assets,
                        current_assets,
                        total_assets,
                        net_assets,
                        net_current_assets,
                        filing_id,
                        filing_format,
                        filing_age_months,
                        last_enriched_at
                    )
                    VALUES
                        (
                            '11111111',
                            NULL, NULL, NULL, 0, FALSE, CAST('[]' AS JSON),
                            NULL, NULL, 75,
                            DATE '2025-01-01', DATE '2025-12-31',
                            800000, 250000, 200000, 3200000, 1800000, 5000000, 2500000, 750000,
                            'file-11111111', 'ixbrl', 6,
                            TIMESTAMP '2026-05-24 12:00:00'
                        ),
                        (
                            '22222222',
                            NULL, NULL, NULL, 0, FALSE, CAST('[]' AS JSON),
                            NULL, NULL, NULL,
                            NULL, NULL,
                            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL,
                            TIMESTAMP '2026-05-24 12:00:00'
                        )
                    """
                )
            finally:
                con.close()

            summary = enrich_revenue(db_path)
            self.assertEqual(summary["estimated"], 1)
            self.assertEqual(summary["skipped"], 1)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                rows = con.execute(
                    """
                    SELECT
                        company_number,
                        revenue,
                        revenue_source,
                        gross_profit,
                        profit_before_tax,
                        profit_after_tax,
                        fixed_assets,
                        current_assets,
                        total_assets,
                        net_assets,
                        net_current_assets,
                        filing_period_start,
                        filing_period_end,
                        filing_id,
                        filing_format,
                        filing_age_months
                    FROM company_enrichment
                    ORDER BY company_number
                    """
                ).fetchall()
                self.assertEqual(
                    rows,
                    [
                        (
                            "11111111",
                            3000000.0,
                            "employee_band_lookup",
                            800000.0,
                            250000.0,
                            200000.0,
                            3200000.0,
                            1800000.0,
                            5000000.0,
                            2500000.0,
                            750000.0,
                            date(2025, 1, 1),
                            date(2025, 12, 31),
                            "file-11111111",
                            "ixbrl",
                            6,
                        ),
                        (
                            "22222222",
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                        ),
                    ],
                )
            finally:
                con.close()

    def test_enrich_directors_preserves_partial_flush_state_on_later_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "directors.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute(
                    """
                    CREATE TABLE companies (
                        company_number TEXT PRIMARY KEY,
                        company_name TEXT,
                        postcode TEXT,
                        address_post_town TEXT,
                        sic_code_1 TEXT,
                        sic_code_2 TEXT,
                        sic_code_3 TEXT,
                        sic_code_4 TEXT
                    )
                    """
                )
                ensure_pipeline_schema(con)
                con.execute(
                    """
                    INSERT INTO companies VALUES
                        ('11111111', 'Alpha', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL),
                        ('22222222', 'Beta', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL),
                        ('33333333', 'Gamma', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL)
                    """
                )
            finally:
                con.close()

            with patch("ch_bulk.companies_house.ch_enricher.CompaniesHouseClient") as client_cls:
                client = client_cls.return_value.__enter__.return_value
                client.get_officers.side_effect = [
                    [{"officer_role": "director", "date_of_birth": {"year": 1960}}],
                    [{"officer_role": "secretary", "date_of_birth": {"year": 1970}}],
                    RuntimeError("boom"),
                ]

                summary = enrich_directors(
                    db_path,
                    tmpdir,
                    batch_size=2,
                )

            self.assertEqual(summary["enriched"], 1)
            self.assertEqual(summary["no_active_directors"], 1)
            self.assertEqual(summary["error_count"], 1)
            self.assertTrue(
                (
                    Path(tmpdir)
                    / "staging"
                    / f"ch_directors_{summary['batch_id']}.jsonl.loaded"
                ).exists()
            )
            current_year = datetime.now(timezone.utc).year

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                rows = con.execute(
                    """
                    SELECT company_number, avg_director_age, directors_over_60
                    FROM company_enrichment
                    ORDER BY company_number
                    """
                ).fetchall()
                self.assertEqual(
                    rows,
                    [
                        ("11111111", current_year - 1960, 1),
                        ("22222222", None, 0),
                    ],
                )

                batch = con.execute(
                    """
                    SELECT sync_type, mode, status, records_fetched, records_updated, error_count
                    FROM cqc_sync_batches
                    ORDER BY started_at DESC
                    LIMIT 1
                    """
                ).fetchone()
                self.assertEqual(
                    batch,
                    ("ch_directors", "incremental", "succeeded", 2, 2, 1),
                )
            finally:
                con.close()

    def test_enrich_directors_writes_line_buffered_log_with_flush_summaries(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "directors.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute(
                    """
                    CREATE TABLE companies (
                        company_number TEXT PRIMARY KEY,
                        company_name TEXT,
                        postcode TEXT,
                        address_post_town TEXT,
                        sic_code_1 TEXT,
                        sic_code_2 TEXT,
                        sic_code_3 TEXT,
                        sic_code_4 TEXT
                    )
                    """
                )
                ensure_pipeline_schema(con)
                con.execute(
                    """
                    INSERT INTO companies VALUES
                        ('11111111', 'Alpha', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL),
                        ('22222222', 'Beta', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL),
                        ('33333333', 'Gamma', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL)
                    """
                )
            finally:
                con.close()

            buffering_values: list[int] = []
            real_open = open

            def tracking_open(*args, **kwargs):
                if str(args[0]).endswith(".log"):
                    buffering_values.append(kwargs.get("buffering"))
                return real_open(*args, **kwargs)

            with patch("ch_bulk.core.logging.open", side_effect=tracking_open):
                with patch("ch_bulk.companies_house.ch_enricher.CompaniesHouseClient") as client_cls:
                    client = client_cls.return_value.__enter__.return_value
                    client.get_officers.side_effect = [
                        [{"officer_role": "director", "date_of_birth": {"year": 1960}}],
                        [{"officer_role": "secretary", "date_of_birth": {"year": 1970}}],
                        [{"officer_role": "director", "date_of_birth": {"year": 1975}}],
                    ]

                    summary = enrich_directors(
                        db_path,
                        tmpdir,
                        batch_size=2,
                    )

            self.assertIn(1, buffering_values)
            self.assertTrue(
                (
                    Path(tmpdir)
                    / "staging"
                    / f"ch_directors_{summary['batch_id']}.jsonl.loaded"
                ).exists()
            )
            log_text = Path(str(summary["log_path"])).read_text(encoding="utf-8")
            self.assertIn("start sync_type=ch_directors", log_text)
            self.assertIn("company_number=11111111 status=ok", log_text)
            self.assertIn("company_number=22222222 status=skip", log_text)
            self.assertEqual(log_text.count("flush batch_size="), 2)
            self.assertIn("complete requested=3", log_text)


if __name__ == "__main__":
    unittest.main()
