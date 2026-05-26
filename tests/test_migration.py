"""Focused tests for Parquet migration bundle export/import."""

from __future__ import annotations

import json
import tempfile
import unittest
import warnings
from pathlib import Path

import duckdb

from ch_bulk.bootstrap import ensure_pipeline_schema
from ch_bulk.migration import (
    MANIFEST_FILENAME,
    PORTABLE_TABLES,
    export_to_parquet,
    import_from_parquet,
)


def _create_companies_table(con: duckdb.DuckDBPyConnection) -> None:
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


def _create_cqc_bulk_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE cqc_providers (
            provider_id TEXT PRIMARY KEY,
            provider_name TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE cqc_locations (
            location_id TEXT PRIMARY KEY,
            provider_id TEXT
        )
        """
    )


class MigrationTests(unittest.TestCase):
    def _seed_bulk_base(self, con: duckdb.DuckDBPyConnection) -> None:
        con.execute(
            """
            INSERT INTO companies VALUES
                ('12345678', 'Example Homecare Ltd', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL)
            """
        )
        con.execute(
            """
            INSERT INTO cqc_providers VALUES
                ('prov-1', 'Example Homecare Ltd')
            """
        )
        con.execute(
            """
            INSERT INTO cqc_locations VALUES
                ('loc-1', 'prov-1')
            """
        )
        con.execute(
            """
            INSERT INTO cqc_hsca_locations (
                location_id,
                provider_id,
                bulk_imported_at,
                bulk_file_date,
                raw_row
            )
            VALUES (
                'loc-1',
                'prov-1',
                TIMESTAMP '2026-05-24 12:00:00',
                DATE '2026-05-24',
                CAST('{"fixture":"loc-1"}' AS JSON)
            )
            """
        )

    def _create_source_db(self, db_path: Path) -> None:
        con = duckdb.connect(str(db_path))
        try:
            _create_companies_table(con)
            _create_cqc_bulk_tables(con)
            ensure_pipeline_schema(con)
            self._seed_bulk_base(con)

            con.execute(
                """
                INSERT INTO exclusion_lists (list_name, source, is_active)
                VALUES ('manual', 'fixture', TRUE)
                """
            )
            con.execute(
                """
                INSERT INTO excluded_companies (company_number, list_name, reason)
                VALUES ('12345678', 'manual', 'fixture')
                """
            )
            con.execute(
                """
                INSERT INTO excluded_staging (
                    row_id,
                    raw_name,
                    resolved_number
                )
                VALUES (7, 'Example Homecare Ltd', '12345678')
                """
            )
            con.execute(
                """
                INSERT INTO cqc_sync_batches VALUES (
                    '00000000-0000-0000-0000-000000000001',
                    'api_providers',
                    'fixture',
                    TIMESTAMP '2026-05-24 12:00:00',
                    TIMESTAMP '2026-05-24 12:00:00',
                    'succeeded',
                    1,
                    1,
                    0
                )
                """
            )
            con.execute(
                """
                INSERT INTO cqc_api_responses (
                    response_id,
                    batch_id,
                    entity_type,
                    entity_id,
                    fetched_at,
                    scrape_date,
                    http_status,
                    raw_json
                )
                VALUES (
                    11,
                    '00000000-0000-0000-0000-000000000001',
                    'provider',
                    'prov-1',
                    TIMESTAMP '2026-05-24 12:05:00',
                    DATE '2026-05-24',
                    200,
                    CAST('{"providerId":"prov-1","rating":"Good"}' AS JSON)
                )
                """
            )
            con.execute(
                """
                INSERT INTO cqc_providers_enriched (
                    provider_id,
                    companies_house_number,
                    company_name,
                    registration_status,
                    api_response_id,
                    enriched_at
                )
                VALUES (
                    'prov-1',
                    '12345678',
                    'Example Homecare Ltd',
                    'Registered',
                    11,
                    TIMESTAMP '2026-05-24 12:06:00'
                )
                """
            )
            con.execute(
                """
                INSERT INTO cqc_locations_enriched (
                    location_id,
                    provider_id,
                    registration_status,
                    api_response_id,
                    enriched_at
                )
                VALUES (
                    'loc-1',
                    'prov-1',
                    'Registered',
                    11,
                    TIMESTAMP '2026-05-24 12:07:00'
                )
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
                    67,
                    61,
                    72,
                    2,
                    TRUE,
                    CAST('[1954,1958]' AS JSON),
                    10000000,
                    'fixture',
                    120,
                    TIMESTAMP '2026-05-24 12:00:00'
                )
                """
            )
            con.execute(
                """
                INSERT INTO classification_batches VALUES (
                    '00000000-0000-0000-0000-000000000010',
                    'llm:test',
                    'website',
                    TIMESTAMP '2026-05-24 12:00:00',
                    TIMESTAMP '2026-05-24 12:01:00',
                    'succeeded',
                    1,
                    1,
                    0,
                    0,
                    'fixture-model'
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
                    NULL,
                    'Provides care at home',
                    'https://example.com',
                    'llm:test',
                    TIMESTAMP '2026-05-24 12:02:00',
                    '00000000-0000-0000-0000-000000000010'
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
                VALUES (
                    '12345678',
                    'prov-1',
                    95,
                    CAST('["fixture"]' AS JSON),
                    'auto_confirmed',
                    TIMESTAMP '2026-05-24 12:03:00'
                )
                """
            )
            con.execute(
                """
                INSERT INTO company_websites (
                    website_id,
                    company_number,
                    discovered_via,
                    url,
                    is_primary,
                    discovered_at
                )
                VALUES (
                    9,
                    '12345678',
                    'manual',
                    'https://example.com',
                    TRUE,
                    TIMESTAMP '2026-05-24 12:04:00'
                )
                """
            )
        finally:
            con.close()

    def _create_import_target_db(self, db_path: Path) -> None:
        con = duckdb.connect(str(db_path))
        try:
            _create_companies_table(con)
            _create_cqc_bulk_tables(con)
            ensure_pipeline_schema(con)
            self._seed_bulk_base(con)
        finally:
            con.close()

    def test_export_import_round_trip_preserves_portable_table_counts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            source_db = tmp_path / "source.duckdb"
            target_db = tmp_path / "target.duckdb"
            bundle_dir = tmp_path / "portable-bundle"
            self._create_source_db(source_db)
            self._create_import_target_db(target_db)

            export_summary = export_to_parquet(source_db, bundle_dir)
            self.assertTrue((bundle_dir / MANIFEST_FILENAME).exists())
            self.assertEqual(
                export_summary["table_files"],
                [f"{name}.parquet" for name in PORTABLE_TABLES],
            )

            manifest = json.loads(
                (bundle_dir / MANIFEST_FILENAME).read_text(encoding="utf-8")
            )
            self.assertEqual(manifest["schema_version"], "1")
            self.assertEqual(manifest["table_files"], export_summary["table_files"])
            self.assertEqual(manifest["row_counts"], export_summary["row_counts"])

            import_summary = import_from_parquet(bundle_dir, target_db)
            self.assertEqual(import_summary["imported_tables"], PORTABLE_TABLES)

            source_con = duckdb.connect(str(source_db), read_only=True)
            target_con = duckdb.connect(str(target_db), read_only=True)
            try:
                for table_name in PORTABLE_TABLES:
                    source_count = source_con.execute(
                        f"SELECT COUNT(*) FROM {table_name}"
                    ).fetchone()[0]
                    target_count = target_con.execute(
                        f"SELECT COUNT(*) FROM {table_name}"
                    ).fetchone()[0]
                    self.assertEqual(
                        target_count,
                        source_count,
                        msg=f"row count mismatch for {table_name}",
                    )
            finally:
                source_con.close()
                target_con.close()

    def test_import_skips_unknown_future_tables_in_manifest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            source_db = tmp_path / "source.duckdb"
            target_db = tmp_path / "target.duckdb"
            bundle_dir = tmp_path / "portable-bundle"
            self._create_source_db(source_db)
            self._create_import_target_db(target_db)

            export_to_parquet(source_db, bundle_dir)

            future_path = bundle_dir / "future_table.parquet"
            con = duckdb.connect()
            try:
                con.execute(
                    f"""
                    COPY (
                        SELECT 1 AS id, 'future' AS label
                    )
                    TO '{future_path}'
                    (
                        FORMAT PARQUET,
                        COMPRESSION SNAPPY
                    )
                    """
                )
            finally:
                con.close()

            manifest_path = bundle_dir / MANIFEST_FILENAME
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["row_counts"]["future_table"] = 1
            manifest["table_files"].append("future_table.parquet")
            manifest_path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )

            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                summary = import_from_parquet(bundle_dir, target_db)

            self.assertIn("future_table", summary["skipped_tables"])
            self.assertTrue(
                any("future_table" in str(warning.message) for warning in caught)
            )

    def test_uuid_and_json_columns_round_trip_from_parquet(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            source_db = tmp_path / "source.duckdb"
            target_db = tmp_path / "target.duckdb"
            bundle_dir = tmp_path / "portable-bundle"
            self._create_source_db(source_db)
            self._create_import_target_db(target_db)

            export_to_parquet(source_db, bundle_dir)
            import_from_parquet(bundle_dir, target_db)

            con = duckdb.connect(str(target_db), read_only=True)
            try:
                response_row = con.execute(
                    """
                    SELECT
                        typeof(batch_id),
                        CAST(batch_id AS VARCHAR),
                        typeof(raw_json),
                        CAST(raw_json AS VARCHAR)
                    FROM cqc_api_responses
                    WHERE response_id = 11
                    """
                ).fetchone()
                self.assertEqual(
                    response_row,
                    (
                        "UUID",
                        "00000000-0000-0000-0000-000000000001",
                        "JSON",
                        '{"providerId":"prov-1","rating":"Good"}',
                    ),
                )
            finally:
                con.close()

    def test_import_requires_non_empty_companies_table(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            source_db = tmp_path / "source.duckdb"
            bundle_dir = tmp_path / "portable-bundle"
            empty_target = tmp_path / "empty-target.duckdb"
            self._create_source_db(source_db)
            export_to_parquet(source_db, bundle_dir)

            con = duckdb.connect(str(empty_target))
            try:
                _create_companies_table(con)
                ensure_pipeline_schema(con)
            finally:
                con.close()

            with self.assertRaisesRegex(RuntimeError, "ch-bulk sync"):
                import_from_parquet(bundle_dir, empty_target)


if __name__ == "__main__":
    unittest.main()
