"""Tests for HSCA browse query helpers."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from ch_bulk.cqc import query as cqc_query
from ch_bulk.cqc.query import _build_cqc_locations_where


class TestWhereAlias(unittest.TestCase):
    def test_default_alias_unqualified(self):
        where, _params = _build_cqc_locations_where(name_contains="acme")
        self.assertIn("LOWER(name) LIKE", where)
        self.assertNotIn("LOWER(l.name)", where)

    def test_alias_qualifies_columns(self):
        where, _params = _build_cqc_locations_where(
            name_contains="acme",
            regions=["London"],
            postcode_prefix="SW1",
            alias="l",
        )
        self.assertIn("LOWER(l.name) LIKE", where)
        self.assertIn("l.region IN", where)
        self.assertIn("STARTS_WITH(l.postcode", where)


class _HscaQueryFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = Path(self.tmpdir.name)
        self.db = self.tmp / "test.duckdb"

        con = duckdb.connect(str(self.db))
        try:
            con.execute(
                """
                CREATE TABLE cqc_locations (
                    location_id TEXT,
                    name TEXT,
                    provider_name TEXT,
                    postcode TEXT,
                    region TEXT,
                    local_authority TEXT,
                    service_types TEXT,
                    is_active BOOLEAN
                )
                """
            )
            con.execute(
                """
                CREATE TABLE cqc_hsca_locations (
                    location_id TEXT,
                    provider_id TEXT,
                    provider_companies_house_number TEXT,
                    provider_ownership_type TEXT,
                    provider_brand_name TEXT,
                    care_home BOOLEAN,
                    number_of_beds INTEGER,
                    dormant BOOLEAN,
                    st_domiciliary_care_service BOOLEAN,
                    st_supported_living_service BOOLEAN,
                    st_care_home_with_nursing BOOLEAN,
                    st_care_home_without_nursing BOOLEAN,
                    st_extra_care_housing_services BOOLEAN,
                    st_hospice_services_at_home BOOLEAN,
                    bulk_imported_at TIMESTAMP,
                    bulk_file_date DATE,
                    raw_row JSON,
                    service_user_bands JSON,
                    regulated_activities JSON
                )
                """
            )
            con.execute(
                """
                CREATE TABLE companies (
                    company_number TEXT,
                    company_name TEXT,
                    company_status TEXT
                )
                """
            )
            con.execute(
                """
                INSERT INTO cqc_locations VALUES
                    ('L1', 'Location 1', 'Provider 1', 'SW1A 1AA', 'London', 'LA1', 'Domiciliary care', TRUE),
                    ('L2', 'Location 2', 'Provider 2', 'EH1 1AA', 'Scotland', 'LA2', 'Supported living', TRUE)
                """
            )
            con.execute(
                """
                INSERT INTO cqc_hsca_locations (
                    location_id, provider_id, provider_companies_house_number,
                    provider_ownership_type, provider_brand_name, care_home,
                    number_of_beds, dormant, st_domiciliary_care_service,
                    st_supported_living_service, st_care_home_with_nursing,
                    st_care_home_without_nursing, st_extra_care_housing_services,
                    st_hospice_services_at_home, bulk_imported_at, bulk_file_date,
                    raw_row
                ) VALUES
                    ('L1', 'P1', 'C1', 'Private', 'Brand 1', FALSE, 10, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, NOW(), DATE '2026-05-29', '{"location_id":"L1"}'),
                    ('L2', 'P2', NULL, 'Charity', 'Brand 2', TRUE, 20, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, NOW(), DATE '2026-05-29', '{"location_id":"L2"}'),
                    ('L3', 'P3', 'C3', 'Private', 'Brand 3', FALSE, 30, TRUE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, NOW(), DATE '2026-05-29', '{"location_id":"L3"}')
                """
            )
            con.execute(
                """
                INSERT INTO companies VALUES
                    ('C1', 'ACME CARE LTD', 'active')
                """
            )
        finally:
            con.close()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()


class TestQueryHsca(_HscaQueryFixture):
    def test_inner_join_excludes_unmatched(self):
        rows, total = cqc_query.query_hsca_locations(self.db)
        ids = {row["location_id"] for row in rows}
        self.assertEqual(ids, {"L1", "L2"})
        self.assertEqual(total, 2)

    def test_company_fields_via_left_join(self):
        rows, _total = cqc_query.query_hsca_locations(self.db)
        by_id = {row["location_id"]: row for row in rows}
        self.assertEqual(by_id["L1"]["company_name"], "ACME CARE LTD")
        self.assertIsNone(by_id["L2"]["company_name"])

    def test_has_ch_number_true(self):
        rows, total = cqc_query.query_hsca_locations(self.db, has_ch_number=True)
        self.assertEqual({row["location_id"] for row in rows}, {"L1"})
        self.assertEqual(total, 1)

    def test_has_ch_number_false(self):
        rows, total = cqc_query.query_hsca_locations(self.db, has_ch_number=False)
        self.assertEqual({row["location_id"] for row in rows}, {"L2"})
        self.assertEqual(total, 1)

    def test_has_ch_number_none_returns_all(self):
        rows, total = cqc_query.query_hsca_locations(self.db, has_ch_number=None)
        self.assertEqual({row["location_id"] for row in rows}, {"L1", "L2"})
        self.assertEqual(total, 2)

    def test_export_writes_rows(self):
        out = self.tmp / "hsca.csv"
        count = cqc_query.export_hsca_locations_csv(self.db, out, has_ch_number=True)
        self.assertEqual(count, 1)
        self.assertIn("company_name", out.read_text().splitlines()[0])


class TestApiHsca(_HscaQueryFixture):
    def test_api_query_hsca(self):
        from ch_bulk import ChBulk

        temp_data_dir = self.tmp / "data"
        temp_data_dir.mkdir()
        ch = ChBulk(data_dir=temp_data_dir, db_path=self.db)
        rows, total = ch.query_hsca_locations_advanced(has_ch_number=True)
        self.assertEqual(total, 1)
        self.assertEqual({row["location_id"] for row in rows}, {"L1"})
