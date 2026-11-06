"""Tests for loading companies missing from the bulk snapshot via the CH profile API."""

from __future__ import annotations

import csv
import tempfile
import threading
import unittest
from datetime import date
from pathlib import Path

import duckdb
import httpx

from ch_bulk.core.cancellation import OperationCancelled
from ch_bulk.companies_house.ch_enricher import CH_API_BASE, CompaniesHouseClient
from ch_bulk.companies_house.company_profile import (
    ProfileLoadResult,
    ensure_companies_table,
    has_bulk_companies,
    load_missing_companies,
    profile_to_companies_row,
)
from ch_bulk.companies_house.processor import process_csvs

# Source columns of the Companies House BasicCompanyData CSV.
_BULK_COLUMNS = [
    "CompanyName", "CompanyNumber",
    "RegAddress.CareOf", "RegAddress.POBox", "RegAddress.AddressLine1",
    "RegAddress.AddressLine2", "RegAddress.PostTown", "RegAddress.County",
    "RegAddress.Country", "RegAddress.PostCode",
    "CompanyCategory", "CompanyStatus", "CountryOfOrigin",
    "DissolutionDate", "IncorporationDate",
    "Accounts.AccountRefDay", "Accounts.AccountRefMonth", "Accounts.NextDueDate",
    "Accounts.LastMadeUpDate", "Accounts.AccountCategory",
    "Returns.NextDueDate", "Returns.LastMadeUpDate",
    "Mortgages.NumMortCharges", "Mortgages.NumMortOutstanding",
    "Mortgages.NumMortPartSatisfied", "Mortgages.NumMortSatisfied",
    "SICCode.SicText_1", "SICCode.SicText_2", "SICCode.SicText_3", "SICCode.SicText_4",
    "LimitedPartnerships.NumGenPartners", "LimitedPartnerships.NumLimPartners",
    "URI",
    *[
        col
        for i in range(1, 11)
        for col in (f"PreviousName_{i}.CONDATE", f"PreviousName_{i}.CompanyName")
    ],
    "ConfStmtNextDueDate", "ConfStmtLastMadeUpDate",
]

_SPEC_KEYS = {
    "company_number", "company_name", "company_status", "company_type",
    "incorporation_date", "dissolution_date",
    "sic_code_1", "sic_code_2", "sic_code_3", "sic_code_4",
    "address_care_of", "address_po_box", "address_line_1", "address_line_2",
    "address_post_town", "address_county", "address_country", "postcode",
    "registered_address",
    "accounts_last_made_up", "accounts_next_due", "accounts_ref_day", "accounts_ref_month",
    "conf_stmt_next_due", "conf_stmt_last_made_up",
    "is_active", "record_source",
}


def _write_bulk_csv(
    directory: Path,
    scrape_date: str,
    companies: list[tuple[str, str]],
) -> Path:
    """Write a minimal BasicCompanyData CSV with (number, name) rows."""
    path = directory / f"BasicCompanyData-{scrape_date}-part1_1.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_BULK_COLUMNS)
        writer.writeheader()
        for number, name in companies:
            writer.writerow({
                "CompanyNumber": number,
                "CompanyName": name,
                "CompanyStatus": "Active",
                "CompanyCategory": "Private Limited Company",
                "RegAddress.AddressLine1": "1 High Street",
                "RegAddress.PostTown": "LONDON",
                "RegAddress.PostCode": "SW1A 1AA",
                "IncorporationDate": "01/02/2015",
                "SICCode.SicText_1": "88100 - Social work activities",
            })
    return path


def _process(csv_path: Path, db_path: Path, **kwargs) -> int:
    return process_csvs(
        [csv_path], db_path, progress_callback=lambda _msg: None, compact=False, **kwargs
    )


def _profile(number: str, name: str = "Api Co Ltd", **overrides) -> dict:
    profile = {
        "company_number": number,
        "company_name": name,
        "company_status": "active",
        "type": "ltd",
        "date_of_creation": "2019-06-10",
        "sic_codes": ["88100"],
        "registered_office_address": {
            "address_line_1": "2 Low Road",
            "locality": "Leeds",
            "postal_code": "LS1 1AA",
        },
    }
    profile.update(overrides)
    return profile


class FakeClient:
    def __init__(self, profiles: dict[str, dict | None], on_call=None):
        self.profiles = profiles
        self.calls: list[str] = []
        self.on_call = on_call

    def get_company_profile(self, company_number, cancel_event=None):
        self.calls.append(company_number)
        if self.on_call is not None:
            self.on_call(company_number)
        return self.profiles.get(company_number)


def _columns(con: duckdb.DuckDBPyConnection, table: str = "companies") -> dict[str, str]:
    return {
        name: dtype
        for name, dtype in con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ? ORDER BY ordinal_position",
            [table],
        ).fetchall()
    }


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()[0] > 0


def _rows(db_path: Path, sql: str, params=None) -> list[tuple]:
    con = duckdb.connect(str(db_path))
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


class _TempDirCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.data_dir = self.tmp / "data"
        self.data_dir.mkdir()
        self.db_path = self.data_dir / "test.duckdb"

    def tearDown(self):
        self._tmp.cleanup()


class ProfileToCompaniesRowTests(unittest.TestCase):
    def _full_profile(self) -> dict:
        return {
            "company_number": "SC012345",
            "company_name": "Highland Care Ltd",
            "company_status": "active",
            "type": "ltd",
            "date_of_creation": "2010-03-15",
            "sic_codes": ["87300", "88100"],
            "registered_office_address": {
                "care_of": "Smith & Co",
                "po_box": "PO Box 12",
                "address_line_1": "3 Glen Road",
                "address_line_2": "",
                "locality": "Inverness",
                "region": "Highland",
                "country": "Scotland",
                "postal_code": "IV1 1AA",
            },
            "accounts": {
                "accounting_reference_date": {"day": "31", "month": "03"},
                "last_accounts": {"made_up_to": "2025-03-31"},
                "next_due": "2026-12-31",
            },
            "confirmation_statement": {
                "next_due": "2026-04-01",
                "last_made_up_to": "2025-03-18",
            },
        }

    def test_full_profile_maps_every_spec_field(self):
        row = profile_to_companies_row(self._full_profile())
        self.assertEqual(set(row), _SPEC_KEYS)
        self.assertEqual(row["company_number"], "SC012345")
        self.assertEqual(row["company_name"], "Highland Care Ltd")
        self.assertEqual(row["company_status"], "Active")
        self.assertEqual(row["company_type"], "Private Limited Company")
        self.assertEqual(row["incorporation_date"], date(2010, 3, 15))
        self.assertIsNone(row["dissolution_date"])
        self.assertEqual(
            [row["sic_code_1"], row["sic_code_2"], row["sic_code_3"], row["sic_code_4"]],
            ["87300", "88100", None, None],
        )
        self.assertEqual(row["address_care_of"], "Smith & Co")
        self.assertEqual(row["address_po_box"], "PO Box 12")
        self.assertEqual(row["address_line_1"], "3 Glen Road")
        self.assertEqual(row["address_post_town"], "Inverness")
        self.assertEqual(row["address_county"], "Highland")
        self.assertEqual(row["address_country"], "Scotland")
        self.assertEqual(row["postcode"], "IV1 1AA")
        self.assertEqual(
            row["registered_address"],
            "Smith & Co, PO Box 12, 3 Glen Road, Inverness, Highland, Scotland",
        )
        self.assertEqual(row["accounts_last_made_up"], date(2025, 3, 31))
        self.assertEqual(row["accounts_next_due"], date(2026, 12, 31))
        self.assertEqual(row["accounts_ref_day"], 31)
        self.assertEqual(row["accounts_ref_month"], 3)
        self.assertEqual(row["conf_stmt_next_due"], date(2026, 4, 1))
        self.assertEqual(row["conf_stmt_last_made_up"], date(2025, 3, 18))
        self.assertIs(row["is_active"], True)
        self.assertEqual(row["record_source"], "api")

    def test_minimal_profile_leaves_optional_fields_none(self):
        row = profile_to_companies_row({"company_number": "01234567", "company_name": "Bare Ltd"})
        self.assertLessEqual(set(row), _SPEC_KEYS)
        self.assertEqual(row["company_number"], "01234567")
        self.assertEqual(row["record_source"], "api")
        self.assertIs(row["is_active"], False)
        for key in _SPEC_KEYS - {"company_number", "company_name", "record_source", "is_active"}:
            with self.subTest(key=key):
                self.assertIsNone(row.get(key))

    def test_registered_address_none_when_all_parts_empty(self):
        row = profile_to_companies_row(_profile(
            "01234567",
            registered_office_address={"address_line_1": "", "postal_code": "AB1 2CD"},
        ))
        self.assertIsNone(row["registered_address"])
        self.assertEqual(row["postcode"], "AB1 2CD")

    def test_dissolution_date(self):
        row = profile_to_companies_row(_profile(
            "01234567", company_status="dissolved", date_of_cessation="2024-11-05",
        ))
        self.assertEqual(row["dissolution_date"], date(2024, 11, 5))
        self.assertEqual(row["company_status"], "Dissolved")
        self.assertIs(row["is_active"], False)

    def test_status_mapping_and_is_active(self):
        cases = [
            ("active", "active-proposal-to-strike-off", "Active - Proposal to Strike off", True),
            ("active", None, "Active", True),
            ("dissolved", None, "Dissolved", False),
            ("liquidation", None, "Liquidation", True),
            ("administration", None, "In Administration", True),
            ("voluntary-arrangement", None, "Voluntary Arrangement", True),
            ("receivership", None, "RECEIVERSHIP", True),
            ("insolvency-proceedings", None, "Insolvency proceedings", True),
            ("converted-closed", None, "Converted closed", False),
            ("removed", None, "Removed", False),
            ("closed", None, "Closed", False),
            ("open", None, "Open", True),
        ]
        for status, detail, expected_status, expected_active in cases:
            with self.subTest(status=status, detail=detail):
                profile = _profile("01234567", company_status=status)
                if detail is not None:
                    profile["company_status_detail"] = detail
                row = profile_to_companies_row(profile)
                self.assertEqual(row["company_status"], expected_status)
                self.assertIs(row["is_active"], expected_active)

    def test_missing_status(self):
        profile = _profile("01234567")
        del profile["company_status"]
        row = profile_to_companies_row(profile)
        self.assertIsNone(row["company_status"])
        self.assertIs(row["is_active"], False)

    def test_type_mapping_with_raw_fallback(self):
        cases = {
            "ltd": "Private Limited Company",
            "plc": "Public Limited Company",
            "llp": "Limited Liability Partnership",
            "limited-partnership": "Limited Partnership",
            "private-limited-guarant-nsc":
                "PRI/LTD BY GUAR/NSC (Private, limited by guarantee, no share capital)",
            "private-limited-guarant-nsc-limited-exemption":
                "PRI/LBG/NSC (Private, Limited by guarantee, no share capital, "
                "use of 'Limited' exemption)",
            "private-unlimited": "Private Unlimited Company",
            "charitable-incorporated-organisation": "Charitable Incorporated Organisation",
            "scottish-charitable-incorporated-organisation":
                "Scottish Charitable Incorporated Organisation",
            "registered-overseas-entity": "Overseas Entity",
            "royal-charter": "Royal Charter Company",
            "scottish-partnership": "Scottish Partnership",
            "old-public-company": "Old Public Company",
            "industrial-and-provident-society": "industrial-and-provident-society",
        }
        for api_type, expected in cases.items():
            with self.subTest(api_type=api_type):
                row = profile_to_companies_row(_profile("01234567", type=api_type))
                self.assertEqual(row["company_type"], expected)

        profile = _profile("01234567")
        del profile["type"]
        self.assertIsNone(profile_to_companies_row(profile)["company_type"])


class CompaniesTableTests(_TempDirCase):
    def _bulk_columns(self) -> dict[str, str]:
        bulk_db = self.tmp / "bulk.duckdb"
        csv_path = _write_bulk_csv(self.tmp, "2026-05-01", [("11111111", "Bulk Ltd")])
        _process(csv_path, bulk_db)
        con = duckdb.connect(str(bulk_db))
        try:
            return _columns(con)
        finally:
            con.close()

    def test_fresh_db_gets_empty_table_matching_bulk_schema(self):
        con = duckdb.connect(str(self.db_path))
        try:
            ensure_companies_table(con)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM companies").fetchone()[0], 0)
            self.assertFalse(_table_exists(con, "companies_staging"))
            api_columns = _columns(con)
        finally:
            con.close()
        self.assertIn("record_source", api_columns)
        self.assertEqual(api_columns, self._bulk_columns())

    def test_mapped_row_keys_are_companies_columns(self):
        con = duckdb.connect(str(self.db_path))
        try:
            ensure_companies_table(con)
            columns = set(_columns(con))
        finally:
            con.close()
        self.assertLessEqual(_SPEC_KEYS, columns)

    def test_fresh_table_enforces_unique_company_number(self):
        con = duckdb.connect(str(self.db_path))
        try:
            ensure_companies_table(con)
            con.execute("INSERT INTO companies (company_number) VALUES ('01234567')")
            with self.assertRaises(duckdb.ConstraintException):
                con.execute("INSERT INTO companies (company_number) VALUES ('01234567')")
        finally:
            con.close()

    def test_existing_table_without_record_source_gets_column_and_keeps_rows(self):
        con = duckdb.connect(str(self.db_path))
        try:
            con.execute("CREATE TABLE companies (company_number TEXT, company_name TEXT)")
            con.execute("INSERT INTO companies VALUES ('01234567', 'Old Ltd')")
            ensure_companies_table(con)
            ensure_companies_table(con)
            self.assertIn("record_source", _columns(con))
            self.assertEqual(
                con.execute("SELECT company_number, company_name, record_source FROM companies").fetchall(),
                [("01234567", "Old Ltd", None)],
            )
        finally:
            con.close()

    def test_has_bulk_companies(self):
        con = duckdb.connect(str(self.db_path))
        try:
            self.assertFalse(has_bulk_companies(con))
            ensure_companies_table(con)
            self.assertFalse(has_bulk_companies(con))
            con.execute(
                "INSERT INTO companies (company_number, record_source) VALUES ('01234567', 'api')"
            )
            self.assertFalse(has_bulk_companies(con))
            con.execute("INSERT INTO companies (company_number) VALUES ('07654321')")
            self.assertTrue(has_bulk_companies(con))
        finally:
            con.close()

    def test_has_bulk_companies_true_after_bulk_process(self):
        csv_path = _write_bulk_csv(self.tmp, "2026-05-01", [("11111111", "Bulk Ltd")])
        _process(csv_path, self.db_path)
        con = duckdb.connect(str(self.db_path))
        try:
            self.assertTrue(has_bulk_companies(con))
        finally:
            con.close()


class LoadMissingCompaniesTests(_TempDirCase):
    def _load(self, numbers, client, **kwargs) -> ProfileLoadResult:
        return load_missing_companies(
            self.db_path, self.data_dir, numbers, client=client, **kwargs
        )

    def test_inserts_found_and_reports_not_found_on_fresh_db(self):
        client = FakeClient({
            "22222222": _profile("22222222", "Second Ltd"),
            "11111111": _profile("11111111", "First Ltd"),
        })
        result = self._load(["22222222", "99999999", "11111111"], client)

        self.assertEqual(result.inserted, ["22222222", "11111111"])
        self.assertEqual(result.not_found, ["99999999"])
        self.assertEqual(result.already_present, [])
        self.assertEqual(client.calls, ["22222222", "99999999", "11111111"])

        rows = _rows(
            self.db_path,
            "SELECT company_number, company_name, company_type, postcode, sic_code_1, "
            "record_source, is_active, first_scrape_date, last_scrape_date, "
            "marked_inactive_scrape_date, last_enriched_at "
            "FROM companies ORDER BY company_number",
        )
        self.assertEqual(rows, [
            ("11111111", "First Ltd", "Private Limited Company", "LS1 1AA", "88100",
             "api", True, None, date.today(), None, None),
            ("22222222", "Second Ltd", "Private Limited Company", "LS1 1AA", "88100",
             "api", True, None, date.today(), None, None),
        ])

    def test_dates_round_trip_as_dates(self):
        client = FakeClient({"11111111": _profile("11111111")})
        self._load(["11111111"], client)
        self.assertEqual(
            _rows(self.db_path, "SELECT incorporation_date FROM companies"),
            [(date(2019, 6, 10),)],
        )

    def test_already_present_api_row_is_skipped_without_refresh(self):
        self._load(["11111111"], FakeClient({"11111111": _profile("11111111", "First Ltd")}))

        client = FakeClient({"11111111": _profile("11111111", "Renamed Ltd")})
        result = self._load(["11111111"], client)

        self.assertEqual(result.already_present, ["11111111"])
        self.assertEqual(result.inserted, [])
        self.assertEqual(client.calls, [])
        self.assertEqual(
            _rows(self.db_path, "SELECT company_name FROM companies"), [("First Ltd",)]
        )

    def test_refresh_replaces_api_row(self):
        self._load(["11111111"], FakeClient({"11111111": _profile("11111111", "First Ltd")}))

        client = FakeClient({"11111111": _profile("11111111", "Renamed Ltd")})
        result = self._load(["11111111"], client, refresh=True)

        self.assertEqual(result.inserted, ["11111111"])
        self.assertEqual(result.already_present, [])
        self.assertEqual(client.calls, ["11111111"])
        self.assertEqual(
            _rows(self.db_path, "SELECT company_name, record_source FROM companies"),
            [("Renamed Ltd", "api")],
        )

    def test_refresh_not_found_keeps_api_row(self):
        self._load(["11111111"], FakeClient({"11111111": _profile("11111111", "First Ltd")}))

        client = FakeClient({})
        result = self._load(["11111111"], client, refresh=True)

        self.assertEqual(result.not_found, ["11111111"])
        self.assertEqual(result.inserted, [])
        self.assertEqual(result.already_present, [])
        self.assertEqual(client.calls, ["11111111"])
        self.assertEqual(
            _rows(self.db_path, "SELECT company_name, record_source FROM companies"),
            [("First Ltd", "api")],
        )

    def test_fresh_bulk_rows_are_not_fetched_or_overwritten(self):
        today = date.today().isoformat()
        csv_path = _write_bulk_csv(self.tmp, today, [("11111111", "Bulk Ltd")])
        _process(csv_path, self.db_path)

        for refresh in (False, True):
            with self.subTest(refresh=refresh):
                client = FakeClient({
                    "11111111": _profile("11111111", "Api Name"),
                    "22222222": _profile("22222222", "Api Only Ltd"),
                })
                result = self._load(["11111111", "22222222"], client, refresh=refresh)
                self.assertEqual(result.already_present, ["11111111"])
                self.assertNotIn("11111111", client.calls)
                self.assertEqual(
                    _rows(
                        self.db_path,
                        "SELECT company_name, record_source, first_scrape_date "
                        "FROM companies WHERE company_number = '11111111'",
                    ),
                    [("Bulk Ltd", None, date.today())],
                )

    def test_bulk_row_over_a_month_old_is_refreshed_and_stays_bulk(self):
        csv_path = _write_bulk_csv(self.tmp, "2026-05-01", [("11111111", "Bulk Ltd")])
        _process(csv_path, self.db_path)
        con = duckdb.connect(str(self.db_path))
        con.execute("UPDATE companies SET uri = 'bulk-only-uri'")
        con.close()

        client = FakeClient({"11111111": _profile("11111111", "Renamed Ltd")})
        result = self._load(["11111111"], client)

        self.assertEqual(client.calls, ["11111111"])
        self.assertEqual(result.inserted, ["11111111"])
        self.assertEqual(
            _rows(
                self.db_path,
                "SELECT company_name, record_source, first_scrape_date, "
                "last_scrape_date, uri FROM companies",
            ),
            [("Renamed Ltd", None, date(2026, 5, 1), date.today(), "bulk-only-uri")],
        )

    def test_api_row_over_a_month_old_is_refetched_without_refresh(self):
        self._load(["11111111"], FakeClient({"11111111": _profile("11111111", "First Ltd")}))
        con = duckdb.connect(str(self.db_path))
        con.execute(
            "UPDATE companies SET first_scrape_date = DATE '2026-01-15', "
            "last_scrape_date = CURRENT_DATE - INTERVAL 1 MONTH - INTERVAL 1 DAY"
        )
        con.close()

        client = FakeClient({"11111111": _profile("11111111", "Renamed Ltd")})
        result = self._load(["11111111"], client)

        self.assertEqual(result.inserted, ["11111111"])
        self.assertEqual(
            _rows(
                self.db_path,
                "SELECT company_name, record_source, first_scrape_date, "
                "last_scrape_date FROM companies",
            ),
            [("Renamed Ltd", "api", date(2026, 1, 15), date.today())],
        )

    def test_api_row_exactly_a_month_old_is_reused(self):
        self._load(["11111111"], FakeClient({"11111111": _profile("11111111", "First Ltd")}))
        con = duckdb.connect(str(self.db_path))
        con.execute("UPDATE companies SET last_scrape_date = CURRENT_DATE - INTERVAL 1 MONTH")
        con.close()

        client = FakeClient({"11111111": _profile("11111111", "Renamed Ltd")})
        result = self._load(["11111111"], client)

        self.assertEqual(client.calls, [])
        self.assertEqual(result.already_present, ["11111111"])

    def test_api_row_without_a_scrape_date_is_refetched(self):
        # API rows stored before fetch dates were recorded.
        self._load(["11111111"], FakeClient({"11111111": _profile("11111111", "First Ltd")}))
        con = duckdb.connect(str(self.db_path))
        con.execute("UPDATE companies SET first_scrape_date = NULL, last_scrape_date = NULL")
        con.close()

        client = FakeClient({"11111111": _profile("11111111", "Renamed Ltd")})
        self._load(["11111111"], client)

        self.assertEqual(client.calls, ["11111111"])
        self.assertEqual(
            _rows(self.db_path, "SELECT first_scrape_date, last_scrape_date FROM companies"),
            [(None, date.today())],
        )

    def test_progress_receives_lines(self):
        lines: list[str] = []
        client = FakeClient({"11111111": _profile("11111111")})
        self._load(["11111111", "22222222"], client, progress=lines.append)
        self.assertGreaterEqual(len(lines), 2)
        self.assertTrue(all(isinstance(line, str) and line for line in lines))

    def test_already_cancelled_raises_without_fetching_or_inserting(self):
        cancel = threading.Event()
        cancel.set()
        client = FakeClient({"11111111": _profile("11111111")})
        with self.assertRaises(OperationCancelled):
            self._load(["11111111"], client, cancel_event=cancel)
        self.assertEqual(client.calls, [])
        if self.db_path.exists():
            con = duckdb.connect(str(self.db_path))
            try:
                if _table_exists(con, "companies"):
                    self.assertEqual(
                        con.execute("SELECT COUNT(*) FROM companies").fetchone()[0], 0
                    )
            finally:
                con.close()

    def test_cancel_between_companies_stops_further_fetches(self):
        cancel = threading.Event()
        client = FakeClient(
            {"11111111": _profile("11111111"), "22222222": _profile("22222222")},
            on_call=lambda _number: cancel.set(),
        )
        with self.assertRaises(OperationCancelled):
            self._load(["11111111", "22222222"], client, cancel_event=cancel)
        self.assertEqual(client.calls, ["11111111"])


class BulkProcessingToleratesApiRowsTests(_TempDirCase):
    def test_api_only_db_then_bulk_process_without_force(self):
        client = FakeClient({
            "11111111": _profile("11111111", "Api One"),
            "22222222": _profile("22222222", "Api Two"),
        })
        load_missing_companies(self.db_path, self.data_dir, ["11111111", "22222222"], client=client)

        csv_path = _write_bulk_csv(
            self.tmp, "2026-05-01",
            [("11111111", "Bulk One"), ("33333333", "Bulk Three")],
        )
        total = _process(csv_path, self.db_path)
        self.assertEqual(total, 3)

        rows = _rows(
            self.db_path,
            "SELECT company_number, company_name, record_source, is_active, "
            "marked_inactive_scrape_date FROM companies ORDER BY company_number",
        )
        self.assertEqual(rows, [
            ("11111111", "Bulk One", None, True, None),
            ("22222222", "Api Two", "api", False, date(2026, 5, 1)),
            ("33333333", "Bulk Three", None, True, None),
        ])
        # The snapshot that first includes an API-loaded company is its first scrape.
        self.assertEqual(
            _rows(
                self.db_path,
                "SELECT first_scrape_date FROM companies WHERE company_number = '11111111'",
            ),
            [(date(2026, 5, 1),)],
        )

    def test_api_rows_do_not_count_toward_sanity_thresholds(self):
        bulk = [(f"{i:08d}", f"Bulk {i}") for i in range(1, 21)]
        _process(_write_bulk_csv(self.tmp, "2026-05-01", bulk), self.db_path)

        api_numbers = [f"{i:08d}" for i in range(101, 106)]
        client = FakeClient({n: _profile(n) for n in api_numbers})
        result = load_missing_companies(self.db_path, self.data_dir, api_numbers, client=client)
        self.assertEqual(result.inserted, api_numbers)

        # Counting the 5 API rows would make both the row-count delta and the
        # inactive churn 20%, over the 5% guard.
        next_dir = self.tmp / "next"
        next_dir.mkdir()
        total = _process(_write_bulk_csv(next_dir, "2026-06-01", bulk), self.db_path)
        self.assertEqual(total, 25)
        self.assertEqual(
            _rows(
                self.db_path,
                "SELECT COUNT(*) FROM companies WHERE record_source = 'api' AND NOT is_active",
            ),
            [(5,)],
        )

    def test_bulk_built_table_has_record_source(self):
        _process(_write_bulk_csv(self.tmp, "2026-05-01", [("11111111", "Bulk")]), self.db_path)
        self.assertEqual(
            _rows(self.db_path, "SELECT record_source FROM companies"), [(None,)]
        )

    def test_old_db_without_record_source_still_processes(self):
        bulk = [("11111111", "Bulk One"), ("22222222", "Bulk Two")]
        _process(_write_bulk_csv(self.tmp, "2026-05-01", bulk), self.db_path)

        con = duckdb.connect(str(self.db_path))
        try:
            old_columns = [c for c in _columns(con) if c != "record_source"]
            con.execute(
                "CREATE TABLE companies_old AS SELECT "
                + ", ".join(f'"{c}"' for c in old_columns)
                + " FROM companies"
            )
            con.execute("DROP TABLE companies")
            con.execute("ALTER TABLE companies_old RENAME TO companies")
            self.assertNotIn("record_source", _columns(con))
        finally:
            con.close()

        next_dir = self.tmp / "next"
        next_dir.mkdir()
        total = _process(_write_bulk_csv(next_dir, "2026-06-01", bulk), self.db_path)
        self.assertEqual(total, 2)
        self.assertEqual(
            _rows(self.db_path, "SELECT record_source, is_active FROM companies"),
            [(None, True), (None, True)],
        )


class GetCompanyProfileTests(unittest.TestCase):
    def _client(self, handler) -> CompaniesHouseClient:
        client = CompaniesHouseClient(api_key="test-key")
        client.close()
        client._client = httpx.Client(base_url=CH_API_BASE, transport=httpx.MockTransport(handler))
        self.addCleanup(client.close)
        return client

    def test_returns_profile_json(self):
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.url.path)
            return httpx.Response(200, json={"company_number": "01234567", "company_name": "X"})

        profile = self._client(handler).get_company_profile("01234567")
        self.assertEqual(profile, {"company_number": "01234567", "company_name": "X"})
        self.assertEqual(seen, ["/company/01234567"])

    def test_404_returns_none(self):
        client = self._client(lambda request: httpx.Response(404, json={"errors": []}))
        self.assertIsNone(client.get_company_profile("99999999"))

    def test_other_errors_propagate(self):
        client = self._client(lambda request: httpx.Response(401))
        with self.assertRaises(httpx.HTTPStatusError):
            client.get_company_profile("01234567")


if __name__ == "__main__":
    unittest.main()
