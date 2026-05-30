"""Tests for enrichment capture + backfill.

Recon notes recorded from the live code before implementation:
- `cqc_api_responses.raw_json` for `entity_type='ch_directors'` stores the
  officers `items` array, not the top-level Companies House response object.
- CQC provider/location runtime persistence uses
  `PROVIDER_ENRICH_INSERT_SQL` / `LOCATION_ENRICH_INSERT_SQL`, not the
  `parse_*_payload()` helpers.
- Provider and location sub-ratings come from
  `currentRatings.overall.keyQuestionRatings[]` when `overall` exists.
- HSCA `raw_row` stores the exact ODS headers from the bulk file.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

import duckdb
from typer.testing import CliRunner

from ch_bulk import ChBulk
from ch_bulk.cli import app as cli_app
from ch_bulk.cqc.api_client import APIResult
from ch_bulk.cqc.api_enricher import CQCAPIEnricher, backfill_cqc_ratings
from ch_bulk.cqc.processor import extract_hsca_flags, backfill_hsca_flags, process_hsca_filters
from ch_bulk.companies_house.ch_enricher import (
    backfill_directors,
    compute_director_meta,
    enrich_directors,
)
from ch_bulk.db.bootstrap import ensure_enrichment_columns, ensure_pipeline_schema

CLI_RUNNER = CliRunner()


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


def _column_types(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> dict[str, str]:
    return {
        row[0]: row[1]
        for row in con.execute(f"DESCRIBE {table_name}").fetchall()
    }


def _insert_sync_batch(
    con: duckdb.DuckDBPyConnection,
    *,
    batch_id: str,
    sync_type: str = "fixture",
) -> None:
    con.execute(
        """
        INSERT INTO cqc_sync_batches (
            batch_id,
            sync_type,
            mode,
            started_at,
            finished_at,
            status,
            records_fetched,
            records_updated,
            error_count
        )
        VALUES (
            CAST(? AS UUID),
            ?,
            'fixture',
            TIMESTAMP '2026-05-29 12:00:00',
            TIMESTAMP '2026-05-29 12:00:00',
            'succeeded',
            0,
            0,
            0
        )
        """,
        [batch_id, sync_type],
    )


def _json_value(value: object) -> object:
    if value is None:
        return None
    return json.loads(str(value))


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


def _snapshot_enrichment_columns(db_path: Path) -> dict[str, list[tuple]]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return {
            "company_enrichment": con.execute(
                """
                SELECT
                    company_number,
                    total_active_directors,
                    directors,
                    avg_director_age,
                    directors_dob_years
                FROM company_enrichment
                ORDER BY company_number
                """
            ).fetchall(),
            "cqc_providers_enriched": con.execute(
                """
                SELECT
                    provider_id,
                    rating_safe,
                    rating_effective,
                    rating_caring,
                    rating_responsive,
                    rating_well_led
                FROM cqc_providers_enriched
                ORDER BY provider_id
                """
            ).fetchall(),
            "cqc_locations_enriched": con.execute(
                """
                SELECT
                    location_id,
                    rating_safe,
                    rating_effective,
                    rating_caring,
                    rating_responsive,
                    rating_well_led
                FROM cqc_locations_enriched
                ORDER BY location_id
                """
            ).fetchall(),
            "cqc_hsca_locations": con.execute(
                """
                SELECT
                    location_id,
                    service_user_bands,
                    regulated_activities
                FROM cqc_hsca_locations
                ORDER BY location_id
                """
            ).fetchall(),
        }
    finally:
        con.close()


class EnrichmentMigrationTests(unittest.TestCase):
    def test_migration_adds_columns_idempotent(self) -> None:
        con = duckdb.connect()
        try:
            con.execute(
                """
                CREATE TABLE company_enrichment (
                    company_number TEXT PRIMARY KEY,
                    last_enriched_at TIMESTAMP NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE cqc_providers_enriched (
                    provider_id TEXT PRIMARY KEY,
                    current_ratings JSON,
                    enriched_at TIMESTAMP NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE cqc_locations_enriched (
                    location_id TEXT PRIMARY KEY,
                    current_ratings JSON,
                    enriched_at TIMESTAMP NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE cqc_hsca_locations (
                    location_id TEXT PRIMARY KEY,
                    provider_id TEXT NOT NULL,
                    bulk_imported_at TIMESTAMP NOT NULL,
                    bulk_file_date DATE NOT NULL,
                    raw_row JSON NOT NULL
                )
                """
            )

            ensure_enrichment_columns(con)
            ensure_enrichment_columns(con)

            company_columns = _column_types(con, "company_enrichment")
            provider_columns = _column_types(con, "cqc_providers_enriched")
            location_columns = _column_types(con, "cqc_locations_enriched")
            hsca_columns = _column_types(con, "cqc_hsca_locations")

            self.assertEqual(company_columns["total_active_directors"], "INTEGER")
            self.assertEqual(company_columns["directors"], "JSON")
            self.assertEqual(provider_columns["rating_safe"], "VARCHAR")
            self.assertEqual(provider_columns["rating_well_led"], "VARCHAR")
            self.assertEqual(location_columns["rating_safe"], "VARCHAR")
            self.assertEqual(location_columns["rating_well_led"], "VARCHAR")
            self.assertEqual(hsca_columns["service_user_bands"], "JSON")
            self.assertEqual(hsca_columns["regulated_activities"], "JSON")
        finally:
            con.close()


class DirectorsBackfillTests(unittest.TestCase):
    def test_compute_director_meta_pure(self) -> None:
        officers = [
            {
                "name": "DIR ONE",
                "officer_role": "director",
                "appointed_on": "2020-01-01",
                "date_of_birth": {"year": 1960},
            },
            {
                "name": "DIR TWO",
                "officer_role": "director",
                "appointed_on": "2021-01-01",
            },
            {
                "name": "DIR THREE",
                "officer_role": "director",
                "appointed_on": "2022-01-01",
                "resigned_on": "2023-01-01",
            },
            {
                "name": "SEC ONE",
                "officer_role": "secretary",
                "appointed_on": "2020-01-01",
            },
        ]

        meta = compute_director_meta(officers)

        self.assertEqual(meta["total_active_directors"], 2)
        self.assertEqual(
            meta["directors"],
            [
                {
                    "name": "DIR ONE",
                    "officer_role": "director",
                    "appointed_on": "2020-01-01",
                },
                {
                    "name": "DIR TWO",
                    "officer_role": "director",
                    "appointed_on": "2021-01-01",
                },
            ],
        )

    def test_backfill_directors_counts_and_json_uses_latest_response(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "directors-backfill.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                _create_companies_table(con)
                ensure_pipeline_schema(con)
                con.execute(
                    """
                    INSERT INTO companies VALUES
                        ('11111111', 'Alpha', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL)
                    """
                )
                _insert_sync_batch(
                    con,
                    batch_id="00000000-0000-0000-0000-000000000001",
                    sync_type="ch_directors",
                )
                old_payload = json.dumps(
                    [
                        {
                            "name": "OLD DIRECTOR",
                            "officer_role": "director",
                            "appointed_on": "2018-01-01",
                            "date_of_birth": {"year": 1950},
                        }
                    ]
                )
                latest_payload = json.dumps(
                    [
                        {
                            "name": "DIR ONE",
                            "officer_role": "director",
                            "appointed_on": "2020-01-01",
                            "date_of_birth": {"year": 1960},
                        },
                        {
                            "name": "DIR TWO",
                            "officer_role": "director",
                            "appointed_on": "2021-01-01",
                            "date_of_birth": {"year": 1970},
                        },
                        {
                            "name": "DIR THREE",
                            "officer_role": "director",
                            "appointed_on": "2022-01-01",
                        },
                        {
                            "name": "OLD RESIGNED",
                            "officer_role": "director",
                            "appointed_on": "2017-01-01",
                            "resigned_on": "2024-01-01",
                            "date_of_birth": {"year": 1955},
                        },
                        {
                            "name": "SEC ONE",
                            "officer_role": "secretary",
                            "appointed_on": "2022-02-01",
                        },
                    ]
                )
                con.execute(
                    """
                    INSERT INTO cqc_api_responses (
                        batch_id,
                        entity_type,
                        entity_id,
                        fetched_at,
                        scrape_date,
                        http_status,
                        raw_json
                    )
                    VALUES
                        (
                            CAST('00000000-0000-0000-0000-000000000001' AS UUID),
                            'ch_directors',
                            '11111111',
                            TIMESTAMP '2026-05-29 12:00:00',
                            DATE '2026-05-29',
                            200,
                            CAST(? AS JSON)
                        ),
                        (
                            CAST('00000000-0000-0000-0000-000000000001' AS UUID),
                            'ch_directors',
                            '11111111',
                            TIMESTAMP '2026-05-29 13:00:00',
                            DATE '2026-05-29',
                            200,
                            CAST(? AS JSON)
                        )
                    """,
                    [old_payload, latest_payload],
                )
            finally:
                con.close()

            summary = backfill_directors(db_path)
            self.assertEqual(summary["records_updated"], 1)

            current_year = datetime.now(timezone.utc).year
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                row = con.execute(
                    """
                    SELECT
                        total_active_directors,
                        directors,
                        avg_director_age,
                        directors_dob_years
                    FROM company_enrichment
                    WHERE company_number = '11111111'
                    """
                ).fetchone()
            finally:
                con.close()

            self.assertEqual(row[0], 3)
            self.assertEqual(
                {director["name"] for director in _json_value(row[1])},
                {"DIR ONE", "DIR TWO", "DIR THREE"},
            )
            self.assertEqual(row[2], int(((current_year - 1960) + (current_year - 1970)) / 2))
            self.assertEqual(_json_value(row[3]), [1960, 1970])

    def test_enrich_directors_go_forward_populates_director_meta(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "directors-go-forward.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                _create_companies_table(con)
                ensure_pipeline_schema(con)
                con.execute(
                    """
                    INSERT INTO companies VALUES
                        ('11111111', 'Alpha', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL)
                    """
                )
            finally:
                con.close()

            with patch("ch_bulk.companies_house.ch_enricher.CompaniesHouseClient") as client_cls:
                client = client_cls.return_value.__enter__.return_value
                client.get_officers.return_value = [
                    {
                        "name": "DIR ONE",
                        "officer_role": "director",
                        "appointed_on": "2020-01-01",
                        "date_of_birth": {"year": 1960},
                    },
                    {
                        "name": "DIR TWO",
                        "officer_role": "director",
                        "appointed_on": "2021-01-01",
                    },
                    {
                        "name": "SEC ONE",
                        "officer_role": "secretary",
                        "appointed_on": "2022-01-01",
                    },
                ]

                summary = enrich_directors(
                    db_path,
                    tmpdir,
                    batch_size=1,
                    company_numbers=["11111111"],
                )

            self.assertEqual(summary["enriched"], 1)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                row = con.execute(
                    """
                    SELECT total_active_directors, directors
                    FROM company_enrichment
                    WHERE company_number = '11111111'
                    """
                ).fetchone()
            finally:
                con.close()

            self.assertEqual(row[0], 2)
            self.assertEqual(
                _json_value(row[1]),
                [
                    {
                        "appointed_on": "2020-01-01",
                        "name": "DIR ONE",
                        "officer_role": "director",
                    },
                    {
                        "appointed_on": "2021-01-01",
                        "name": "DIR TWO",
                        "officer_role": "director",
                    },
                ],
            )


class CQCRatingsBackfillTests(unittest.TestCase):
    def test_backfill_cqc_ratings_populates_columns_without_service_rollup(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ratings-backfill.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                ensure_pipeline_schema(con)
                provider_with_overall = json.dumps(
                    {
                        "overall": {
                            "keyQuestionRatings": [
                                {"name": "Safe", "rating": "Good"},
                                {"name": "Effective", "rating": "Good"},
                                {"name": "Caring", "rating": "Outstanding"},
                                {"name": "Responsive", "rating": "Requires improvement"},
                                {"name": "Well-led", "rating": "Good"},
                            ]
                        },
                        "serviceRatings": [
                            {
                                "name": "Service A",
                                "keyQuestionRatings": [
                                    {"name": "Safe", "rating": "Inadequate"}
                                ],
                            }
                        ],
                    }
                )
                provider_service_only = json.dumps(
                    {
                        "serviceRatings": [
                            {
                                "name": "Service A",
                                "keyQuestionRatings": [
                                    {"name": "Safe", "rating": "Good"}
                                ],
                            }
                        ]
                    }
                )
                location_with_overall = json.dumps(
                    {
                        "overall": {
                            "keyQuestionRatings": [
                                {"name": "Safe", "rating": "Good"},
                                {"name": "Effective", "rating": "Outstanding"},
                                {"name": "Caring", "rating": "Good"},
                                {"name": "Responsive", "rating": "Good"},
                                {"name": "Well-led", "rating": "Good"},
                            ]
                        }
                    }
                )
                con.execute(
                    """
                    INSERT INTO cqc_providers_enriched (
                        provider_id,
                        current_ratings,
                        enriched_at
                    )
                    VALUES
                        ('prov-overall', CAST(? AS JSON), TIMESTAMP '2026-05-29 12:00:00'),
                        ('prov-services-only', CAST(? AS JSON), TIMESTAMP '2026-05-29 12:00:00')
                    """,
                    [provider_with_overall, provider_service_only],
                )
                con.execute(
                    """
                    INSERT INTO cqc_locations_enriched (
                        location_id,
                        current_ratings,
                        enriched_at
                    )
                    VALUES
                        ('loc-overall', CAST(? AS JSON), TIMESTAMP '2026-05-29 12:00:00'),
                        ('loc-no-overall', CAST('{}' AS JSON), TIMESTAMP '2026-05-29 12:00:00')
                    """,
                    [location_with_overall],
                )
            finally:
                con.close()

            summary = backfill_cqc_ratings(db_path)
            self.assertEqual(summary["records_updated"], 4)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                provider_rows = con.execute(
                    """
                    SELECT
                        provider_id,
                        rating_safe,
                        rating_effective,
                        rating_caring,
                        rating_responsive,
                        rating_well_led
                    FROM cqc_providers_enriched
                    ORDER BY provider_id
                    """
                ).fetchall()
                location_rows = con.execute(
                    """
                    SELECT
                        location_id,
                        rating_safe,
                        rating_effective,
                        rating_caring,
                        rating_responsive,
                        rating_well_led
                    FROM cqc_locations_enriched
                    ORDER BY location_id
                    """
                ).fetchall()
            finally:
                con.close()

            self.assertEqual(
                provider_rows,
                [
                    (
                        "prov-overall",
                        "Good",
                        "Good",
                        "Outstanding",
                        "Requires improvement",
                        "Good",
                    ),
                    (
                        "prov-services-only",
                        None,
                        None,
                        None,
                        None,
                        None,
                    ),
                ],
            )
            self.assertEqual(
                location_rows,
                [
                    (
                        "loc-no-overall",
                        None,
                        None,
                        None,
                        None,
                        None,
                    ),
                    (
                        "loc-overall",
                        "Good",
                        "Outstanding",
                        "Good",
                        "Good",
                        "Good",
                    ),
                ],
            )

    def test_enrich_provider_and_location_capture_key_question_ratings(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ratings-go-forward.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                ensure_pipeline_schema(con)
                con.execute("CREATE TABLE cqc_providers (provider_id TEXT PRIMARY KEY)")
                con.execute(
                    "CREATE TABLE cqc_locations (location_id TEXT PRIMARY KEY, provider_id TEXT)"
                )
                con.execute("INSERT INTO cqc_providers VALUES ('prov-1')")
                con.execute("INSERT INTO cqc_locations VALUES ('loc-1', 'prov-1')")
            finally:
                con.close()

            provider_payload = {
                "providerId": "prov-1",
                "name": "Provider 1",
                "registrationStatus": "Registered",
                "currentRatings": {
                    "overall": {
                        "rating": "Good",
                        "keyQuestionRatings": [
                            {"name": "Safe", "rating": "Good"},
                            {"name": "Effective", "rating": "Outstanding"},
                            {"name": "Caring", "rating": "Good"},
                            {"name": "Responsive", "rating": "Good"},
                            {"name": "Well-led", "rating": "Requires improvement"},
                        ],
                    }
                },
                "regulatedActivities": [],
                "relationships": [],
                "locationIds": ["loc-1"],
            }
            location_payload = {
                "locationId": "loc-1",
                "providerId": "prov-1",
                "registrationStatus": "Registered",
                "currentRatings": {
                    "overall": {
                        "rating": "Good",
                        "keyQuestionRatings": [
                            {"name": "Safe", "rating": "Outstanding"},
                            {"name": "Effective", "rating": "Good"},
                            {"name": "Caring", "rating": "Good"},
                            {"name": "Responsive", "rating": "Good"},
                            {"name": "Well-led", "rating": "Good"},
                        ],
                    }
                },
                "regulatedActivities": [],
                "relationships": [],
            }

            with patch("ch_bulk.cqc.api_enricher.CQCAPIClient") as client_cls:
                client = client_cls.return_value.__enter__.return_value
                client.get_provider.return_value = APIResult(200, provider_payload)
                client.get_location.return_value = APIResult(200, location_payload)

                enricher = CQCAPIEnricher(tmpdir, db_path)
                provider_summary = enricher.enrich_providers(batch_size=1)
                location_summary = enricher.enrich_locations(batch_size=1)

            self.assertEqual(provider_summary["records_updated"], 1)
            self.assertEqual(location_summary["records_updated"], 1)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                provider_row = con.execute(
                    """
                    SELECT rating_safe, rating_effective, rating_caring, rating_responsive, rating_well_led
                    FROM cqc_providers_enriched
                    WHERE provider_id = 'prov-1'
                    """
                ).fetchone()
                location_row = con.execute(
                    """
                    SELECT rating_safe, rating_effective, rating_caring, rating_responsive, rating_well_led
                    FROM cqc_locations_enriched
                    WHERE location_id = 'loc-1'
                    """
                ).fetchone()
            finally:
                con.close()

            self.assertEqual(
                provider_row,
                ("Good", "Outstanding", "Good", "Good", "Requires improvement"),
            )
            self.assertEqual(
                location_row,
                ("Outstanding", "Good", "Good", "Good", "Good"),
            )


class HSCABackfillTests(unittest.TestCase):
    def test_extract_hsca_flags(self) -> None:
        flags = extract_hsca_flags(
            {
                "Service user band - Dementia": "Y",
                "Service user band - Older People": "YES",
                "Service user band - Mental Health": "",
                "Regulated activity - Personal care": "Y",
                "Regulated activity - Nursing care": "N",
            }
        )

        self.assertEqual(flags["service_user_bands"], ["Dementia", "Older People"])
        self.assertEqual(flags["regulated_activities"], ["Personal care"])

    def test_backfill_hsca_flags_updates_arrays(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "hsca-backfill.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                ensure_pipeline_schema(con)
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
                        TIMESTAMP '2026-05-29 12:00:00',
                        DATE '2026-05-29',
                        CAST(? AS JSON)
                    )
                    """,
                    [
                        json.dumps(
                            {
                                "Service user band - Dementia": "Y",
                                "Service user band - Older People": "Y",
                                "Service user band - Mental Health": "",
                                "Regulated activity - Personal care": "Y",
                                "Regulated activity - Nursing care": "",
                            }
                        )
                    ],
                )
            finally:
                con.close()

            summary = backfill_hsca_flags(db_path)
            self.assertEqual(summary["records_updated"], 1)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                row = con.execute(
                    """
                    SELECT service_user_bands, regulated_activities
                    FROM cqc_hsca_locations
                    WHERE location_id = 'loc-1'
                    """
                ).fetchone()
            finally:
                con.close()

            self.assertEqual(_json_value(row[0]), ["Dementia", "Older People"])
            self.assertEqual(_json_value(row[1]), ["Personal care"])

    def test_process_hsca_filters_go_forward_populates_flag_arrays(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            db_path = tmp_path / "hsca-go-forward.duckdb"
            ods_path = tmp_path / "hsca_active_locations_2026-05-05.ods"
            _write_ods_fixture(
                ods_path,
                hsca_rows=[
                    {
                        "Location ID": "loc-1",
                        "Provider ID": "prov-1",
                        "Provider Companies House Number": "00000001",
                        "Provider Charity Number": "",
                        "Provider Ownership Type": "Organisation",
                        "Brand ID": "brand-1",
                        "Brand Name": "Brand 1",
                        "Provider Web Address": "https://provider-1.example",
                        "Location Web Address": "https://location-1.example",
                        "Care home?": "Y",
                        "Care homes beds": "12",
                        "Dormant (Y/N)": "N",
                        "Registered manager": "Manager 1",
                        "Service type - Domiciliary care service": "Y",
                        "Service type - Supported living service": "N",
                        "Service type - Care home service with nursing": "N",
                        "Service type - Care home service without nursing": "Y",
                        "Service type - Extra Care housing services": "N",
                        "Service type - Hospice services at home": "N",
                        "Service user band - Dementia": "Y",
                        "Service user band - Older People": "Y",
                        "Service user band - Mental Health": "",
                        "Regulated activity - Personal care": "Y",
                        "Regulated activity - Nursing care": "",
                    }
                ],
                dual_rows=[
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
                        "Primary ID": "Y",
                    }
                ],
            )

            row_count = process_hsca_filters(
                ods_path,
                db_path,
                compact=False,
                scrape_date_override=date(2026, 5, 5),
            )
            self.assertEqual(row_count, 1)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                row = con.execute(
                    """
                    SELECT service_user_bands, regulated_activities
                    FROM cqc_hsca_locations
                    WHERE location_id = 'loc-1'
                    """
                ).fetchone()
            finally:
                con.close()

            self.assertEqual(_json_value(row[0]), ["Dementia", "Older People"])
            self.assertEqual(_json_value(row[1]), ["Personal care"])


class UnifiedBackfillEntryPointTests(unittest.TestCase):
    def test_backfill_enrichment_runs_all_and_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            db_path = tmp_path / "backfill-entrypoint.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                _create_companies_table(con)
                ensure_pipeline_schema(con)
                con.execute(
                    """
                    INSERT INTO companies VALUES
                        ('11111111', 'Alpha', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL)
                    """
                )
                _insert_sync_batch(
                    con,
                    batch_id="00000000-0000-0000-0000-000000000001",
                    sync_type="ch_directors",
                )
                con.execute(
                    """
                    INSERT INTO cqc_api_responses (
                        batch_id,
                        entity_type,
                        entity_id,
                        fetched_at,
                        scrape_date,
                        http_status,
                        raw_json
                    )
                    VALUES (
                        CAST('00000000-0000-0000-0000-000000000001' AS UUID),
                        'ch_directors',
                        '11111111',
                        TIMESTAMP '2026-05-29 13:00:00',
                        DATE '2026-05-29',
                        200,
                        CAST(? AS JSON)
                    )
                    """,
                    [
                        json.dumps(
                            [
                                {
                                    "name": "DIR ONE",
                                    "officer_role": "director",
                                    "appointed_on": "2020-01-01",
                                    "date_of_birth": {"year": 1960},
                                },
                                {
                                    "name": "DIR TWO",
                                    "officer_role": "director",
                                    "appointed_on": "2021-01-01",
                                },
                            ]
                        )
                    ],
                )
                con.execute(
                    """
                    INSERT INTO cqc_providers_enriched (
                        provider_id,
                        current_ratings,
                        enriched_at
                    )
                    VALUES (
                        'prov-1',
                        CAST(? AS JSON),
                        TIMESTAMP '2026-05-29 12:00:00'
                    )
                    """,
                    [
                        json.dumps(
                            {
                                "overall": {
                                    "keyQuestionRatings": [
                                        {"name": "Safe", "rating": "Good"},
                                        {"name": "Effective", "rating": "Good"},
                                        {"name": "Caring", "rating": "Good"},
                                        {"name": "Responsive", "rating": "Good"},
                                        {"name": "Well-led", "rating": "Good"},
                                    ]
                                }
                            }
                        )
                    ],
                )
                con.execute(
                    """
                    INSERT INTO cqc_locations_enriched (
                        location_id,
                        current_ratings,
                        enriched_at
                    )
                    VALUES (
                        'loc-1',
                        CAST(? AS JSON),
                        TIMESTAMP '2026-05-29 12:00:00'
                    )
                    """,
                    [
                        json.dumps(
                            {
                                "overall": {
                                    "keyQuestionRatings": [
                                        {"name": "Safe", "rating": "Outstanding"},
                                        {"name": "Effective", "rating": "Good"},
                                        {"name": "Caring", "rating": "Good"},
                                        {"name": "Responsive", "rating": "Good"},
                                        {"name": "Well-led", "rating": "Good"},
                                    ]
                                }
                            }
                        )
                    ],
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
                        TIMESTAMP '2026-05-29 12:00:00',
                        DATE '2026-05-29',
                        CAST(? AS JSON)
                    )
                    """,
                    [
                        json.dumps(
                            {
                                "Service user band - Dementia": "Y",
                                "Regulated activity - Personal care": "Y",
                            }
                        )
                    ],
                )
            finally:
                con.close()

            ch = ChBulk(data_dir=tmp_path, db_path=db_path)
            summary = ch.backfill_enrichment()

            self.assertIn("directors_updated", summary)
            self.assertIn("ratings_updated", summary)
            self.assertIn("hsca_updated", summary)
            self.assertEqual(summary["directors_updated"], 1)
            self.assertEqual(summary["ratings_updated"], 2)
            self.assertEqual(summary["hsca_updated"], 1)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                row = con.execute(
                    """
                    SELECT total_active_directors
                    FROM company_enrichment
                    WHERE company_number = '11111111'
                    """
                ).fetchone()
                self.assertEqual(row[0], 2)
            finally:
                con.close()

    def test_backfill_enrichment_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            db_path = tmp_path / "backfill-idempotent.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                _create_companies_table(con)
                ensure_pipeline_schema(con)
                con.execute(
                    """
                    INSERT INTO companies VALUES
                        ('11111111', 'Alpha', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL)
                    """
                )
                _insert_sync_batch(
                    con,
                    batch_id="00000000-0000-0000-0000-000000000001",
                    sync_type="ch_directors",
                )
                con.execute(
                    """
                    INSERT INTO cqc_api_responses (
                        batch_id,
                        entity_type,
                        entity_id,
                        fetched_at,
                        scrape_date,
                        http_status,
                        raw_json
                    )
                    VALUES (
                        CAST('00000000-0000-0000-0000-000000000001' AS UUID),
                        'ch_directors',
                        '11111111',
                        TIMESTAMP '2026-05-29 13:00:00',
                        DATE '2026-05-29',
                        200,
                        CAST(? AS JSON)
                    )
                    """,
                    [
                        json.dumps(
                            [
                                {
                                    "name": "DIR ONE",
                                    "officer_role": "director",
                                    "appointed_on": "2020-01-01",
                                    "date_of_birth": {"year": 1960},
                                }
                            ]
                        )
                    ],
                )
                con.execute(
                    """
                    INSERT INTO cqc_providers_enriched (
                        provider_id,
                        current_ratings,
                        enriched_at
                    )
                    VALUES (
                        'prov-1',
                        CAST(? AS JSON),
                        TIMESTAMP '2026-05-29 12:00:00'
                    )
                    """,
                    [
                        json.dumps(
                            {
                                "overall": {
                                    "keyQuestionRatings": [
                                        {"name": "Safe", "rating": "Good"},
                                        {"name": "Effective", "rating": "Good"},
                                        {"name": "Caring", "rating": "Good"},
                                        {"name": "Responsive", "rating": "Good"},
                                        {"name": "Well-led", "rating": "Good"},
                                    ]
                                }
                            }
                        )
                    ],
                )
                con.execute(
                    """
                    INSERT INTO cqc_locations_enriched (
                        location_id,
                        current_ratings,
                        enriched_at
                    )
                    VALUES (
                        'loc-1',
                        CAST(? AS JSON),
                        TIMESTAMP '2026-05-29 12:00:00'
                    )
                    """,
                    [
                        json.dumps(
                            {
                                "overall": {
                                    "keyQuestionRatings": [
                                        {"name": "Safe", "rating": "Good"},
                                        {"name": "Effective", "rating": "Good"},
                                        {"name": "Caring", "rating": "Good"},
                                        {"name": "Responsive", "rating": "Good"},
                                        {"name": "Well-led", "rating": "Good"},
                                    ]
                                }
                            }
                        )
                    ],
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
                        TIMESTAMP '2026-05-29 12:00:00',
                        DATE '2026-05-29',
                        CAST(? AS JSON)
                    )
                    """,
                    [
                        json.dumps(
                            {
                                "Service user band - Dementia": "Y",
                                "Regulated activity - Personal care": "Y",
                            }
                        )
                    ],
                )
            finally:
                con.close()

            ch = ChBulk(data_dir=tmp_path, db_path=db_path)
            ch.backfill_enrichment()
            first = _snapshot_enrichment_columns(db_path)
            ch.backfill_enrichment()
            second = _snapshot_enrichment_columns(db_path)
            self.assertEqual(first, second)

    def test_migration_backfill_enrichment_cli_invokes_chbulk(self) -> None:
        with patch("ch_bulk.cli.ChBulk") as mock_ch:
            mock_ch.return_value.backfill_enrichment.return_value = {
                "directors_updated": 1,
                "ratings_updated": 2,
                "hsca_updated": 3,
            }
            result = CLI_RUNNER.invoke(
                cli_app,
                [
                    "migration",
                    "backfill-enrichment",
                    "--data-dir",
                    "/tmp",
                    "--db-path",
                    "/tmp/test.duckdb",
                ],
            )

        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        mock_ch.return_value.backfill_enrichment.assert_called_once_with()
        self.assertIn("directors=1", result.stdout)
        self.assertIn("ratings=2", result.stdout)
        self.assertIn("hsca=3", result.stdout)


if __name__ == "__main__":
    unittest.main()
