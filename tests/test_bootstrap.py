"""Focused tests for the Phase 3-6 bootstrap schema."""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.companies_house.processor import compact_database


def _foreign_key_refs(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> set[str]:
    return {
        row[0]
        for row in con.execute(
            """
            SELECT referenced_table
            FROM duckdb_constraints()
            WHERE table_name = ?
              AND constraint_type = 'FOREIGN KEY'
            ORDER BY referenced_table
            """,
            [table_name],
        ).fetchall()
    }


class BootstrapSchemaTests(unittest.TestCase):
    def test_bootstrap_is_idempotent_and_defers_views_until_companies_exist(self):
        con = duckdb.connect()
        try:
            ensure_pipeline_schema(con)
            ensure_pipeline_schema(con)

            relations = dict(
                con.execute(
                    """
                    SELECT table_name, table_type
                    FROM information_schema.tables
                    WHERE table_schema = current_schema()
                    """
                ).fetchall()
            )
            self.assertIn("cqc_hsca_locations", relations)
            self.assertIn("cqc_hsca_dual_registrations", relations)
            self.assertIn("company_websites", relations)
            self.assertNotIn("tiered_targets", relations)

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
            ensure_pipeline_schema(con)

            relations = dict(
                con.execute(
                    """
                    SELECT table_name, table_type
                    FROM information_schema.tables
                    WHERE table_schema = current_schema()
                    """
                ).fetchall()
            )
            self.assertEqual(relations["tiered_targets"], "VIEW")

            columns = {
                row[0]: row[1]
                for row in con.execute(
                    "DESCRIBE cqc_hsca_locations"
                ).fetchall()
            }
            enrichment_columns = {
                row[0]: row[1]
                for row in con.execute(
                    "DESCRIBE company_enrichment"
                ).fetchall()
            }
            self.assertEqual(columns["location_id"], "VARCHAR")
            self.assertEqual(columns["provider_id"], "VARCHAR")
            self.assertEqual(columns["raw_row"], "JSON")
            self.assertEqual(columns["st_domiciliary_care_service"], "BOOLEAN")
            self.assertEqual(enrichment_columns["gross_profit"], "DOUBLE")
            self.assertEqual(enrichment_columns["profit_before_tax"], "DOUBLE")
            self.assertEqual(enrichment_columns["profit_after_tax"], "DOUBLE")
            self.assertEqual(enrichment_columns["filing_period_start"], "DATE")
            self.assertEqual(enrichment_columns["filing_period_end"], "DATE")
            self.assertEqual(enrichment_columns["fixed_assets"], "DOUBLE")
            self.assertEqual(enrichment_columns["current_assets"], "DOUBLE")
            self.assertEqual(enrichment_columns["total_assets"], "DOUBLE")
            self.assertEqual(enrichment_columns["net_assets"], "DOUBLE")
            self.assertEqual(enrichment_columns["net_current_assets"], "DOUBLE")
            self.assertEqual(
                _foreign_key_refs(con, "cqc_providers_enriched"),
                {"cqc_api_responses"},
            )
            self.assertEqual(
                _foreign_key_refs(con, "cqc_locations_enriched"),
                {"cqc_api_responses"},
            )
            self.assertEqual(
                _foreign_key_refs(con, "classifications"),
                {"classification_batches"},
            )
        finally:
            con.close()

    def test_tiered_targets_uses_sql_scoring_and_match_priority(self):
        con = duckdb.connect()
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
                    ('12345678', 'Acme Homecare Ltd', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL)
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
                    last_enriched_at
                )
                VALUES (
                    '12345678',
                    68,
                    64,
                    72,
                    2,
                    TRUE,
                    CAST('[1954,1958]' AS JSON),
                    10000000,
                    'employee_band_lookup',
                    120,
                    TIMESTAMP '2026-05-24 12:00:00'
                )
                """
            )
            con.execute(
                """
                INSERT INTO classifications (
                    company_number,
                    source_type,
                    verdict,
                    verdict_reason,
                    evidence_quote,
                    source_url,
                    classifier,
                    classified_at,
                    batch_id
                )
                VALUES (
                    '12345678',
                    'website',
                    'Majority domiciliary',
                    'fixture',
                    'Provides care at home',
                    'https://example.com',
                    'rule:test',
                    TIMESTAMP '2026-05-24 12:01:00',
                    NULL
                )
                """
            )
            con.execute(
                """
                INSERT INTO ch_cqc_matches (
                    company_number,
                    cqc_provider_id,
                    total_score,
                    match_signals,
                    status,
                    matched_at
                )
                VALUES
                    (
                        '12345678',
                        'prov-auto',
                        90,
                        CAST('["fuzzy_name_outward_pc"]' AS JSON),
                        'auto_confirmed',
                        TIMESTAMP '2026-05-24 12:02:00'
                    ),
                    (
                        '12345678',
                        'prov-user',
                        80,
                        CAST('["manual_override"]' AS JSON),
                        'user_confirmed',
                        TIMESTAMP '2026-05-24 12:03:00'
                    )
                """
            )

            row = con.execute(
                """
                SELECT
                    cqc_provider_id,
                    class_pts,
                    age_pts,
                    size_pts,
                    total_score,
                    tier
                FROM tiered_targets
                """
            ).fetchone()

            self.assertEqual(row, ("prov-user", 3, 3, 2, 8, "Tier 1"))
        finally:
            con.close()

    def test_company_websites_allows_many_false_rows_but_only_one_primary(self):
        con = duckdb.connect()
        try:
            ensure_pipeline_schema(con)
            con.execute(
                """
                INSERT INTO company_websites (
                    company_number,
                    discovered_via,
                    url,
                    is_primary,
                    discovered_at
                )
                VALUES
                    ('12345678', 'manual', 'https://a.example', FALSE, TIMESTAMP '2026-05-24 12:00:00'),
                    ('12345678', 'manual', 'https://b.example', FALSE, TIMESTAMP '2026-05-24 12:00:00'),
                    ('12345678', 'manual', 'https://c.example', TRUE,  TIMESTAMP '2026-05-24 12:00:00')
                """
            )

            with self.assertRaises(duckdb.ConstraintException):
                con.execute(
                    """
                    INSERT INTO company_websites (
                        company_number,
                        discovered_via,
                        url,
                        is_primary,
                        discovered_at
                    )
                    VALUES (
                        '12345678',
                        'manual',
                        'https://d.example',
                        TRUE,
                        TIMESTAMP '2026-05-24 12:05:00'
                    )
                    """
                )
        finally:
            con.close()

    def test_compaction_migrates_old_hsca_fk_shape_and_preserves_data(self):
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "compact.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute(
                    """
                    CREATE TABLE cqc_hsca_locations (
                        location_id TEXT PRIMARY KEY,
                        provider_id TEXT
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE cqc_hsca_dual_registrations (
                        location_id TEXT NOT NULL REFERENCES cqc_hsca_locations(location_id),
                        location_name TEXT,
                        location_hsca_start_date DATE,
                        location_type_sector TEXT,
                        provider_id TEXT NOT NULL,
                        provider_name TEXT,
                        linked_organisation_id TEXT NOT NULL,
                        linked_organisation_name TEXT,
                        relationship TEXT,
                        relationship_start_date DATE,
                        primary_id BOOLEAN,
                        PRIMARY KEY (location_id, provider_id, linked_organisation_id)
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO cqc_hsca_locations VALUES
                        ('loc-1', 'prov-1'),
                        ('loc-2', 'prov-2')
                    """
                )
                con.execute(
                    """
                    INSERT INTO cqc_hsca_dual_registrations VALUES
                        ('loc-1', 'Location 1', DATE '2026-05-05', 'Social Care Org', 'prov-1', 'Provider 1', 'linked-1', 'Linked Org 1', 'Dual Registration', DATE '2026-05-05', TRUE)
                    """
                )
            finally:
                con.close()

            compact_database(db_path)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                rows = con.execute(
                    "SELECT COUNT(*) FROM cqc_hsca_dual_registrations"
                ).fetchone()[0]
                self.assertEqual(rows, 1)
                fk_count = con.execute(
                    """
                    SELECT COUNT(*)
                    FROM duckdb_constraints()
                    WHERE table_name = 'cqc_hsca_dual_registrations'
                      AND constraint_type = 'FOREIGN KEY'
                    """
                ).fetchone()[0]
                self.assertEqual(fk_count, 0)
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
