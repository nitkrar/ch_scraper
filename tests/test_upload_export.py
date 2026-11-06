"""Tests for building and writing the Upload List profile workbook."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

import duckdb
import openpyxl

from ch_bulk.companies_house.company_profile import ProfileLoadResult, ensure_companies_table
from ch_bulk.companies_house.upload_export import (
    PROFILE_COLUMNS,
    build_profile_rows,
    write_profile_xlsx,
    write_template_xlsx,
)
from ch_bulk.companies_house.upload_list import UploadInput, read_company_numbers
from ch_bulk.db.bootstrap import ensure_pipeline_schema

SPEC_COLUMNS = [
    "Company Number", "Company Name", "Incorporation Date", "Company Status", "Directors",
    "Total Active Directors", "Directors 60+", "Min Director Age", "Avg Director Age",
    "Max Director Age", "Employees", "Debtors", "Current Assets", "Creditors Within One Year",
    "Creditors After One Year", "Provisions for Liabilities", "Deferred Tax", "Net Assets",
    "Total Assets", "Revenue", "Profit Before Tax", "Accounts Period End", "Accounts Source",
    "Notes",
]
BALANCE_SHEET_COLUMNS = {
    "Debtors": "debtors",
    "Creditors Within One Year": "creditors_within_one_year",
    "Creditors After One Year": "creditors_after_one_year",
    "Provisions for Liabilities": "provisions_for_liabilities",
    "Deferred Tax": "deferred_tax",
}
MONEY_COLUMNS = ["Current Assets", "Total Assets", "Net Assets", "Revenue", "Profit Before Tax"]

_ENRICHMENT_DEFAULTS = {
    "avg_director_age": None,
    "min_director_age": None,
    "max_director_age": None,
    "directors_over_60": None,
    "directors_dob_years": None,
    "total_active_directors": None,
    "directors": None,
    "revenue": None,
    "revenue_source": None,
    "employee_count": None,
    "filing_period_end": None,
    "profit_before_tax": None,
    "current_assets": None,
    "total_assets": None,
    "net_assets": None,
    "filing_id": None,
    "filing_format": None,
}


def _director(name: str, dob_year: int | None, *, legacy: bool = False) -> dict:
    entry = {"name": name, "officer_role": "director", "appointed_on": "2015-01-01"}
    if not legacy:
        entry["date_of_birth"] = None if dob_year is None else {"month": 6, "year": dob_year}
    return entry


class _DbCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.db_path = self.tmp / "test.duckdb"
        con = duckdb.connect(str(self.db_path))
        try:
            ensure_companies_table(con)
        finally:
            con.close()

    def tearDown(self):
        self._tmp.cleanup()

    def add_company(self, number, name="Acme Ltd", inc=date(2015, 2, 1), status="Active"):
        con = duckdb.connect(str(self.db_path))
        try:
            con.execute(
                "INSERT INTO companies (company_number, company_name, incorporation_date, "
                "company_status) VALUES (?, ?, ?, ?)",
                [number, name, inc, status],
            )
        finally:
            con.close()

    def add_enrichment(self, number, **fields):
        values = {**_ENRICHMENT_DEFAULTS, **fields}
        for key in ("directors", "directors_dob_years"):
            if values[key] is not None and not isinstance(values[key], str):
                values[key] = json.dumps(values[key])
        con = duckdb.connect(str(self.db_path))
        try:
            ensure_pipeline_schema(con)
            columns = ["company_number", *values, "last_enriched_at"]
            con.execute(
                f"INSERT INTO company_enrichment ({', '.join(columns)}) "
                f"VALUES ({', '.join('?' for _ in columns)})",
                [number, *values.values(), datetime(2026, 1, 1)],
            )
        finally:
            con.close()

    def build(self, numbers, *, invalid=(), not_found=(), current_year=2026):
        upload = UploadInput(numbers=list(numbers), invalid=list(invalid), column="Company Number")
        load = ProfileLoadResult(inserted=[], not_found=list(not_found), already_present=[])
        return build_profile_rows(self.db_path, upload, load, current_year=current_year)

    def by_number(self, rows):
        return {row["Company Number"]: row for row in rows}


class ProfileColumnsTests(unittest.TestCase):
    def test_column_order_matches_spec(self):
        self.assertEqual(list(PROFILE_COLUMNS), SPEC_COLUMNS)


class BuildProfileRowsTests(_DbCase):
    def test_rows_follow_input_order_then_invalid_values(self):
        for number in ("22222222", "11111111"):
            self.add_company(number)
        rows = self.build(
            ["22222222", "99999999", "11111111"],
            invalid=["bad!", "ABC12345"],
            not_found=["99999999"],
        )
        self.assertEqual(
            [row["Company Number"] for row in rows],
            ["22222222", "99999999", "11111111", "bad!", "ABC12345"],
        )
        for row in rows:
            self.assertEqual(set(row), set(SPEC_COLUMNS))

    def test_invalid_row_has_only_number_and_note(self):
        rows = self.build([], invalid=["bad!"])
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["Company Number"], "bad!")
        self.assertEqual(row["Notes"], "Invalid company number")
        for column in SPEC_COLUMNS[1:-1]:
            with self.subTest(column=column):
                self.assertIsNone(row[column])

    def test_not_found_row_has_only_number_and_note(self):
        rows = self.build(["99999999"], not_found=["99999999"])
        row = rows[0]
        self.assertEqual(row["Notes"], "Not found at Companies House")
        for column in SPEC_COLUMNS[1:-1]:
            with self.subTest(column=column):
                self.assertIsNone(row[column])

    def test_full_row_values_and_types(self):
        self.add_company("01234567", name="Acme Care Ltd", inc=date(2010, 3, 15), status="Active")
        self.add_enrichment(
            "01234567",
            directors=[_director("SMITH, John", 1960), _director("JONES, Ann", 1990)],
            directors_dob_years=[1960, 1990],
            total_active_directors=2,
            directors_over_60=1,
            min_director_age=36,
            avg_director_age=51,
            max_director_age=66,
            employee_count=42,
            current_assets=1000.0,
            total_assets=5000.0,
            net_assets=2500.0,
            revenue=123456.0,
            revenue_source="filed_accounts_ixbrl",
            profit_before_tax=789.0,
            filing_period_end=date(2025, 3, 31),
            filing_id="F1",
            filing_format="ixbrl",
        )
        row = self.build(["01234567"])[0]
        self.assertEqual(row, {
            "Company Number": "01234567",
            "Company Name": "Acme Care Ltd",
            "Incorporation Date": date(2010, 3, 15),
            "Company Status": "Active",
            "Directors": "SMITH, John (66); JONES, Ann (36)",
            "Total Active Directors": 2,
            "Directors 60+": 1,
            "Min Director Age": 36,
            "Avg Director Age": 51,
            "Max Director Age": 66,
            "Employees": 42,
            "Debtors": None,
            "Current Assets": 1000.0,
            "Creditors Within One Year": None,
            "Creditors After One Year": None,
            "Provisions for Liabilities": None,
            "Deferred Tax": None,
            "Net Assets": 2500.0,
            "Total Assets": 5000.0,
            "Revenue": 123456.0,
            "Profit Before Tax": 789.0,
            "Accounts Period End": date(2025, 3, 31),
            "Accounts Source": "Digital accounts",
            "Notes": None,
        })
        self.assertIs(type(row["Incorporation Date"]), date)
        self.assertIs(type(row["Accounts Period End"]), date)

    def test_balance_sheet_columns_carry_stored_values(self):
        self.add_company("01234567")
        values = {field: 1000.0 * (i + 1) for i, field in enumerate(BALANCE_SHEET_COLUMNS.values())}
        self.add_enrichment("01234567", filing_id="F1", filing_format="ixbrl", **values)
        row = self.build(["01234567"])[0]
        for column, field in BALANCE_SHEET_COLUMNS.items():
            with self.subTest(column=column):
                self.assertEqual(row[column], values[field])

    def test_director_age_uses_current_year(self):
        self.add_company("01234567")
        self.add_enrichment(
            "01234567", directors=[_director("SMITH, John", 1960)], directors_dob_years=[1960],
            total_active_directors=1, avg_director_age=60,
        )
        row = self.build(["01234567"], current_year=2030)[0]
        self.assertEqual(row["Directors"], "SMITH, John (70)")

    def test_directors_without_date_of_birth_show_name_only(self):
        self.add_company("01234567")
        self.add_enrichment(
            "01234567",
            directors=[
                _director("NULL DOB", None),
                _director("LEGACY ENTRY", None, legacy=True),
                _director("HAS DOB", 1980),
            ],
            directors_dob_years=[1980],
            total_active_directors=3,
            avg_director_age=46,
        )
        row = self.build(["01234567"])[0]
        self.assertEqual(row["Directors"], "NULL DOB; LEGACY ENTRY; HAS DOB (46)")

    def test_directors_none_for_empty_or_null_json(self):
        self.add_company("11111111")
        self.add_company("22222222")
        self.add_enrichment("11111111", directors=[], directors_dob_years=[], total_active_directors=0)
        self.add_enrichment("22222222", directors=None)
        rows = self.by_number(self.build(["11111111", "22222222"]))
        self.assertIsNone(rows["11111111"]["Directors"])
        self.assertIsNone(rows["22222222"]["Directors"])

    def test_estimated_revenue_is_blanked(self):
        self.add_company("01234567")
        self.add_enrichment(
            "01234567", revenue=50000.0, revenue_source="employee_band_lookup",
            employee_count=10, filing_id="F1", filing_format="ixbrl",
            directors_dob_years=[1970], total_active_directors=1, avg_director_age=56,
        )
        row = self.build(["01234567"])[0]
        self.assertIsNone(row["Revenue"])
        self.assertEqual(row["Employees"], 10)
        self.assertEqual(row["Notes"], "Revenue not in filed accounts")

    def test_accounts_source_mapping(self):
        cases = {
            "11111111": ("ixbrl", "Digital accounts"),
            "22222222": ("pdf", "PDF"),
            "33333333": ("ocr_pdf", "Scanned PDF (OCR)"),
            "44444444": (None, None),
            "55555555": ("xml", None),
        }
        for number, (filing_format, _) in cases.items():
            self.add_company(number)
            self.add_enrichment(number, filing_format=filing_format)
        rows = self.by_number(self.build(list(cases)))
        for number, (filing_format, expected) in cases.items():
            with self.subTest(filing_format=filing_format):
                self.assertEqual(rows[number]["Accounts Source"], expected)

    def test_missing_enrichment_table_gives_none_enrichment_fields(self):
        self.add_company("01234567", name="Bare Ltd")
        row = self.build(["01234567"])[0]
        self.assertEqual(row["Company Name"], "Bare Ltd")
        for column in SPEC_COLUMNS[4:-1]:
            with self.subTest(column=column):
                self.assertIsNone(row[column])

    def test_notes_rules(self):
        fetched = {"directors_dob_years": [1970], "total_active_directors": 1, "avg_director_age": 56}
        cases = [
            # (enrichment fields or None for no row, expected notes)
            (None, "Directors not fetched; Accounts not fetched"),
            ({}, "Directors not fetched; Accounts not fetched"),
            ({"directors_dob_years": [], "total_active_directors": 0,
              "revenue_source": "no_recent_filing"},
             "No active directors; No accounts filed"),
            ({"directors_dob_years": [], "total_active_directors": 2,
              "revenue_source": "pdf_no_text_layer", "filing_id": "F1", "filing_format": "pdf"},
             "No director dates of birth; Scanned PDF accounts - figures pending OCR"),
            ({**fetched, "filing_id": "F1", "revenue": 1000.0,
              "revenue_source": "filed_accounts_ixbrl"},
             None),
            ({**fetched, "filing_id": "F1", "revenue_source": "partial_no_revenue"},
             "Revenue not in filed accounts"),
            ({**fetched, "filing_id": "F1", "revenue": 500.0,
              "revenue_source": "employee_band_lookup"},
             "Revenue not in filed accounts"),
            # A filing was found but nothing could be read from it.
            ({**fetched, "filing_id": "F1", "filing_format": "ixbrl"},
             "Accounts found but could not be read"),
            ({**fetched}, "Accounts not fetched"),
            ({**fetched, "revenue": 500.0, "revenue_source": "employee_band_lookup"},
             "Accounts not fetched"),
            ({"directors_dob_years": None, "filing_id": "F1", "revenue": 1.0,
              "revenue_source": "filed_accounts_ixbrl"},
             "Directors not fetched"),
        ]
        numbers = []
        for i, (fields, _) in enumerate(cases):
            number = f"{i + 1:08d}"
            numbers.append(number)
            self.add_company(number)
            if fields is not None:
                self.add_enrichment(number, **fields)
        rows = self.by_number(self.build(numbers))
        for number, (fields, expected) in zip(numbers, cases):
            with self.subTest(fields=fields):
                self.assertEqual(rows[number]["Notes"], expected)


class WriteProfileXlsxTests(_DbCase):
    def _rows(self):
        return [
            {
                **{c: None for c in SPEC_COLUMNS},
                "Company Number": "01234567",
                "Company Name": "Acme Ltd",
                "Incorporation Date": date(2010, 3, 15),
                "Total Active Directors": 2,
                "Current Assets": 1000.0,
                "Total Assets": 5000.0,
                "Net Assets": 2500.0,
                "Revenue": 1234567.0,
                "Profit Before Tax": -50.0,
                "Accounts Period End": date(2025, 3, 31),
            },
            {**{c: None for c in SPEC_COLUMNS}, "Company Number": "bad!",
             "Notes": "Invalid company number"},
        ]

    def test_workbook_layout_and_formats(self):
        out = self.tmp / "out.xlsx"
        returned = write_profile_xlsx(self._rows(), out)
        self.assertEqual(Path(returned), out)

        wb = openpyxl.load_workbook(out)
        self.assertEqual(wb.sheetnames, ["Companies"])
        ws = wb["Companies"]
        header = [cell.value for cell in ws[1]]
        self.assertEqual(header, SPEC_COLUMNS)
        self.assertTrue(all(cell.font.bold for cell in ws[1]))
        self.assertEqual(ws.freeze_panes, "A2")
        self.assertEqual(ws.max_row, 3)

        col = {name: i + 1 for i, name in enumerate(SPEC_COLUMNS)}
        number_cell = ws.cell(row=2, column=col["Company Number"])
        self.assertEqual(number_cell.value, "01234567")
        self.assertEqual(number_cell.data_type, "s")

        for name, expected in (("Incorporation Date", date(2010, 3, 15)),
                               ("Accounts Period End", date(2025, 3, 31))):
            with self.subTest(column=name):
                cell = ws.cell(row=2, column=col[name])
                self.assertTrue(cell.is_date)
                self.assertEqual(cell.value.date() if isinstance(cell.value, datetime) else cell.value,
                                 expected)
                self.assertEqual(cell.number_format, "yyyy-mm-dd")

        for name in MONEY_COLUMNS:
            with self.subTest(column=name):
                cell = ws.cell(row=2, column=col[name])
                self.assertIsInstance(cell.value, (int, float))
                self.assertEqual(cell.number_format, "#,##0")
        self.assertEqual(ws.cell(row=2, column=col["Revenue"]).value, 1234567)
        self.assertEqual(ws.cell(row=2, column=col["Total Active Directors"]).value, 2)

        self.assertEqual(ws.cell(row=3, column=col["Company Number"]).value, "bad!")
        self.assertEqual(ws.cell(row=3, column=col["Notes"]).value, "Invalid company number")
        self.assertIsNone(ws.cell(row=3, column=col["Company Name"]).value)

    def test_built_rows_round_trip_through_workbook(self):
        self.add_company("00000123", name="Leading Zeros Ltd")
        rows = self.build(["00000123"], invalid=["oops"])
        out = write_profile_xlsx(rows, self.tmp / "built.xlsx")
        ws = openpyxl.load_workbook(out)["Companies"]
        self.assertEqual([c.value for c in ws["A"]], ["Company Number", "00000123", "oops"])
        self.assertEqual(ws["B2"].value, "Leading Zeros Ltd")


class WriteTemplateXlsxTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_template_contents_and_text_format(self):
        out = self.tmp / "template.xlsx"
        returned = write_template_xlsx(out)
        self.assertEqual(Path(returned), out)
        wb = openpyxl.load_workbook(out)
        self.assertEqual(wb.sheetnames, ["Companies"])
        ws = wb["Companies"]
        self.assertEqual(ws["A1"].value, "Company Number")
        self.assertEqual(ws["A2"].value, "01234567")
        self.assertEqual(ws["A2"].data_type, "s")
        # Column-level "@" so numbers typed into new rows keep leading zeros.
        self.assertEqual(ws.column_dimensions["A"].number_format, "@")

    def test_template_round_trips_through_reader(self):
        out = write_template_xlsx(self.tmp / "template.xlsx")
        result = read_company_numbers(out)
        self.assertEqual(result.column, "Company Number")
        self.assertEqual(result.numbers, ["01234567"])
        self.assertEqual(result.invalid, [])


if __name__ == "__main__":
    unittest.main()
