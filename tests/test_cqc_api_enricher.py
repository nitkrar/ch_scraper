"""Focused tests for CQC API payload parsing and batch enrichment."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from ch_bulk.core.paths import run_stage_file
from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.cqc.api_client import APIResult
from ch_bulk.cqc.api_enricher import (
    CQCAPIEnricher,
    parse_location_payload,
    parse_provider_payload,
)


class PayloadParseTests(unittest.TestCase):
    def test_parse_provider_payload_maps_expected_fields(self):
        payload = {
            "providerId": "prov-1",
            "companiesHouseNumber": "06944493",
            "ownershipType": "Organisation",
            "name": "Orchids Care Limited",
            "registrationDate": "2020-12-09",
            "registrationStatus": "Registered",
            "postalAddressLine1": "53 High Road",
            "postalAddressLine2": "Warmsworth",
            "postalAddressTownCity": "Doncaster",
            "postalCode": "DN4 9LX",
            "region": "Yorkshire & Humberside",
            "localAuthority": "Doncaster",
            "onspdLatitude": 53.4999312,
            "onspdLongitude": -1.1801764,
            "mainPhoneNumber": "01302570729",
            "website": "www.orchids-care.co.uk",
            "inspectionDirectorate": "Adult social care",
            "lastInspection": {"date": "2022-06-23"},
            "regulatedActivities": [
                {
                    "name": "Personal care",
                    "nominatedIndividual": {
                        "personTitle": "Ms",
                        "personGivenName": "Sarah Lyndsey",
                        "personFamilyName": "Robson",
                    },
                }
            ],
            "currentRatings": {"overall": {"rating": "Good"}},
            "locationIds": ["loc-1", "loc-2"],
        }
        row = parse_provider_payload(payload, 7)
        self.assertEqual(row["provider_id"], "prov-1")
        self.assertEqual(row["companies_house_number"], "06944493")
        self.assertEqual(row["nominated_individual"], "Ms Sarah Lyndsey Robson")
        self.assertEqual(row["number_of_locations"], 2)
        self.assertEqual(row["current_overall_rating"], "Good")
        self.assertEqual(row["api_response_id"], 7)

    def test_parse_location_payload_maps_manager_and_primary_category(self):
        payload = {
            "locationId": "loc-1",
            "providerId": "prov-1",
            "careHome": "N",
            "numberOfBeds": 0,
            "dormancy": "N",
            "registrationDate": "2020-12-09",
            "registrationStatus": "Registered",
            "postalAddressLine1": "53 High Road",
            "postalAddressLine2": "Warmsworth",
            "postalAddressTownCity": "Doncaster",
            "postalCode": "DN4 9LX",
            "region": "Yorkshire & Humberside",
            "localAuthority": "Doncaster",
            "onspdLatitude": 53.4999312,
            "onspdLongitude": -1.1801764,
            "uprn": "100051981017",
            "mainPhoneNumber": "01302570729",
            "website": "www.orchids-care.co.uk",
            "inspectionDirectorate": "Adult social care",
            "inspectionCategories": [
                {"primary": "true", "name": "Community based adult social care services"}
            ],
            "regulatedActivities": [
                {
                    "contacts": [
                        {
                            "personTitle": "Ms",
                            "personGivenName": "Sarah Lyndsey",
                            "personFamilyName": "Robson",
                            "personRoles": ["Registered Manager"],
                        }
                    ]
                }
            ],
            "currentRatings": {"overall": {"rating": "Good"}},
        }
        row = parse_location_payload(payload, 9)
        self.assertEqual(row["location_id"], "loc-1")
        self.assertEqual(row["care_home"], False)
        self.assertEqual(row["registered_manager_name"], "Ms Sarah Lyndsey Robson")
        self.assertEqual(
            row["primary_inspection_category"],
            "Community based adult social care services",
        )
        self.assertEqual(row["current_overall_rating"], "Good")
        self.assertEqual(row["api_response_id"], 9)


class CQCAPIEnricherTests(unittest.TestCase):
    def test_enrich_providers_writes_batch_and_enriched_row(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
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
                    CREATE TABLE cqc_providers (
                        provider_id TEXT PRIMARY KEY,
                        provider_name TEXT,
                        active_location_count INTEGER,
                        total_location_count INTEGER,
                        service_types_list VARCHAR[],
                        regions_list VARCHAR[],
                        postcode_prefixes_list VARCHAR[],
                        local_authorities_list VARCHAR[],
                        first_scrape_date DATE,
                        last_scrape_date DATE,
                        is_active BOOLEAN,
                        marked_inactive_scrape_date DATE
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO cqc_providers VALUES
                        ('prov-1', 'Provider 1', 1, 1, [], [], [], [], DATE '2026-05-01', DATE '2026-05-01', TRUE, NULL)
                    """
                )
            finally:
                con.close()

            fake_payload = {
                "providerId": "prov-1",
                "name": "Provider 1",
                "registrationStatus": "Registered",
                "registrationDate": "2020-12-09",
                "regulatedActivities": [],
                "relationships": [],
                "locationIds": ["loc-1"],
            }

            with patch("ch_bulk.cqc.api_enricher.CQCAPIClient") as client_cls:
                client = client_cls.return_value.__enter__.return_value
                client.get_provider.return_value = APIResult(200, fake_payload)

                summary = CQCAPIEnricher(tmpdir, db_path).enrich_providers(batch_size=2)

            self.assertEqual(summary["records_updated"], 1)
            self.assertTrue(str(summary["log_path"]).endswith(".log"))
            self.assertTrue(
                Path(
                    f"{run_stage_file(tmpdir, 'api_providers', str(summary['batch_id']))}.loaded"
                ).exists()
            )

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                provider = con.execute(
                    """
                    SELECT provider_id, company_name, registration_status, number_of_locations
                    FROM cqc_providers_enriched
                    """
                ).fetchone()
                self.assertEqual(provider, ("prov-1", "Provider 1", "Registered", 1))

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
                    ("api_providers", "incremental", "succeeded", 1, 1, 0),
                )
            finally:
                con.close()

    def test_enrich_providers_preserves_partial_flush_state_on_later_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
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
                    CREATE TABLE cqc_providers (
                        provider_id TEXT PRIMARY KEY,
                        provider_name TEXT,
                        active_location_count INTEGER,
                        total_location_count INTEGER,
                        service_types_list VARCHAR[],
                        regions_list VARCHAR[],
                        postcode_prefixes_list VARCHAR[],
                        local_authorities_list VARCHAR[],
                        first_scrape_date DATE,
                        last_scrape_date DATE,
                        is_active BOOLEAN,
                        marked_inactive_scrape_date DATE
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO cqc_providers VALUES
                        ('prov-1', 'Provider 1', 1, 1, [], [], [], [], DATE '2026-05-01', DATE '2026-05-01', TRUE, NULL),
                        ('prov-2', 'Provider 2', 1, 1, [], [], [], [], DATE '2026-05-01', DATE '2026-05-01', TRUE, NULL),
                        ('prov-3', 'Provider 3', 1, 1, [], [], [], [], DATE '2026-05-01', DATE '2026-05-01', TRUE, NULL)
                    """
                )
            finally:
                con.close()

            payload_1 = {
                "providerId": "prov-1",
                "name": "Provider 1",
                "registrationStatus": "Registered",
                "registrationDate": "2020-12-09",
                "regulatedActivities": [],
                "relationships": [],
                "locationIds": ["loc-1"],
            }
            payload_2 = {
                "providerId": "prov-2",
                "name": "Provider 2",
                "registrationStatus": "Registered",
                "registrationDate": "2020-12-09",
                "regulatedActivities": [],
                "relationships": [],
                "locationIds": ["loc-2"],
            }

            with patch("ch_bulk.cqc.api_enricher.CQCAPIClient") as client_cls:
                client = client_cls.return_value.__enter__.return_value
                client.get_provider.side_effect = [
                    APIResult(200, payload_1),
                    APIResult(200, payload_2),
                    RuntimeError("boom"),
                ]

                summary = CQCAPIEnricher(tmpdir, db_path).enrich_providers(
                    mode="all",
                    batch_size=2,
                )

            self.assertEqual(summary["records_fetched"], 2)
            self.assertEqual(summary["records_updated"], 2)
            self.assertEqual(summary["error_count"], 1)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                provider_rows = con.execute(
                    """
                    SELECT provider_id, company_name
                    FROM cqc_providers_enriched
                    ORDER BY provider_id
                    """
                ).fetchall()
                self.assertEqual(
                    provider_rows,
                    [("prov-1", "Provider 1"), ("prov-2", "Provider 2")],
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
                    ("api_providers", "all", "succeeded", 2, 2, 1),
                )
            finally:
                con.close()

    def test_enrich_providers_writes_line_buffered_log_with_flush_summaries(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
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
                    CREATE TABLE cqc_providers (
                        provider_id TEXT PRIMARY KEY,
                        provider_name TEXT,
                        active_location_count INTEGER,
                        total_location_count INTEGER,
                        service_types_list VARCHAR[],
                        regions_list VARCHAR[],
                        postcode_prefixes_list VARCHAR[],
                        local_authorities_list VARCHAR[],
                        first_scrape_date DATE,
                        last_scrape_date DATE,
                        is_active BOOLEAN,
                        marked_inactive_scrape_date DATE
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO cqc_providers VALUES
                        ('prov-1', 'Provider 1', 1, 1, [], [], [], [], DATE '2026-05-01', DATE '2026-05-01', TRUE, NULL),
                        ('prov-2', 'Provider 2', 1, 1, [], [], [], [], DATE '2026-05-01', DATE '2026-05-01', TRUE, NULL),
                        ('prov-3', 'Provider 3', 1, 1, [], [], [], [], DATE '2026-05-01', DATE '2026-05-01', TRUE, NULL)
                    """
                )
            finally:
                con.close()

            payloads = [
                {
                    "providerId": "prov-1",
                    "name": "Provider 1",
                    "registrationStatus": "Registered",
                    "registrationDate": "2020-12-09",
                    "regulatedActivities": [],
                    "relationships": [],
                    "locationIds": ["loc-1"],
                },
                {
                    "providerId": "prov-2",
                    "name": "Provider 2",
                    "registrationStatus": "Registered",
                    "registrationDate": "2020-12-09",
                    "regulatedActivities": [],
                    "relationships": [],
                    "locationIds": ["loc-2"],
                },
                {
                    "providerId": "prov-3",
                    "name": "Provider 3",
                    "registrationStatus": "Registered",
                    "registrationDate": "2020-12-09",
                    "regulatedActivities": [],
                    "relationships": [],
                    "locationIds": ["loc-3"],
                },
            ]

            buffering_values: list[int] = []
            real_open = open

            def tracking_open(*args, **kwargs):
                if str(args[0]).endswith(".log"):
                    buffering_values.append(kwargs.get("buffering"))
                return real_open(*args, **kwargs)

            with patch("ch_bulk.core.logging.open", side_effect=tracking_open):
                with patch("ch_bulk.cqc.api_enricher.CQCAPIClient") as client_cls:
                    client = client_cls.return_value.__enter__.return_value
                    client.get_provider.side_effect = [
                        APIResult(200, payload) for payload in payloads
                    ]

                    summary = CQCAPIEnricher(tmpdir, db_path).enrich_providers(
                        mode="all",
                        batch_size=2,
                    )

            self.assertIn(1, buffering_values)
            self.assertTrue(
                Path(
                    f"{run_stage_file(tmpdir, 'api_providers', str(summary['batch_id']))}.loaded"
                ).exists()
            )
            log_text = Path(str(summary["log_path"])).read_text(encoding="utf-8")
            self.assertIn("start sync_type=api_providers", log_text)
            self.assertIn("entity_id=prov-1 status=ok", log_text)
            self.assertIn("entity_id=prov-3 status=ok", log_text)
            self.assertEqual(log_text.count("flush batch_size="), 2)
            self.assertIn("complete requested=3", log_text)


if __name__ == "__main__":
    unittest.main()
