"""Tests for reading an uploaded list of company numbers (CSV / xlsx)."""

from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

import openpyxl

from ch_bulk.companies_house.upload_list import (
    UploadInput,
    normalise_company_number,
    read_company_numbers,
)


def _write_csv(path: Path, rows: list[list[object]]) -> Path:
    with path.open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerows(rows)
    return path


def _write_xlsx(path: Path, rows: list[list[object]], extra_sheet_rows=None) -> Path:
    wb = openpyxl.Workbook()
    ws = wb.active
    for row in rows:
        ws.append(row)
    if extra_sheet_rows is not None:
        other = wb.create_sheet("Other")
        for row in extra_sheet_rows:
            other.append(row)
    wb.save(path)
    return path


class NormaliseCompanyNumberTests(unittest.TestCase):
    def test_valid_shapes(self):
        cases = {
            "01234567": "01234567",
            "1234567": "01234567",
            "1": "00000001",
            " 1234 567 ": "01234567",
            "SC12345": "SC012345",
            "sc 012345": "SC012345",
            "SC012345": "SC012345",
            "R123": "R0000123",
            "oc300001": "OC300001",
            "NI000001": "NI000001",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(normalise_company_number(raw), expected)

    def test_excel_numeric_cells(self):
        self.assertEqual(normalise_company_number(1234567), "01234567")
        self.assertEqual(normalise_company_number(12345678), "12345678")
        self.assertEqual(normalise_company_number(1234567.0), "01234567")
        self.assertIsNone(normalise_company_number(1234567.5))
        self.assertIsNone(normalise_company_number(123456789))

    def test_empty_values_are_none(self):
        for raw in (None, "", "   ", "\t"):
            with self.subTest(raw=raw):
                self.assertIsNone(normalise_company_number(raw))

    def test_invalid_shapes_are_none(self):
        for raw in (
            "123456789",     # 9 digits
            "SC1234567",     # 9 chars
            "ABC12345",      # 3 leading letters
            "1234567A",      # letter after digits
            "12A45678",      # letter inside digits
            "12-345",        # punctuation
            "SC/12345",
            "ACME LTD",
            "SC",            # letters with no digits
        ):
            with self.subTest(raw=raw):
                self.assertIsNone(normalise_company_number(raw))


class ReadCompanyNumbersTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_csv_with_alias_header_reads_only_that_column(self):
        path = _write_csv(self.tmp / "list.csv", [
            ["Name", "Company Number", "Phone"],
            ["Acme", "1234567", "12345678"],
            ["Beta", "sc 12345", "87654321"],
        ])
        result = read_company_numbers(path)
        self.assertIsInstance(result, UploadInput)
        self.assertEqual(result.numbers, ["01234567", "SC012345"])
        self.assertEqual(result.invalid, [])
        self.assertEqual(result.column, "Company Number")

    def test_header_aliases_are_matched_after_stripping_separators(self):
        for header in (
            "company_number", "Company No.", "COMPANY-NO", "Companies House Number",
            "Companies House No", "CH Number", "CH No", "CRN",
            "Company Registration Number", "Registration Number", "Reg No",
            "Registered Number", "Companies Number", "Company No #",
        ):
            with self.subTest(header=header):
                path = _write_csv(self.tmp / "alias.csv", [
                    ["Name", header],
                    ["Acme", "1234567"],
                ])
                result = read_company_numbers(path)
                self.assertEqual(result.column, header)
                self.assertEqual(result.numbers, ["01234567"])

    def test_first_matching_header_cell_wins(self):
        # Reading chosen: "first match" is the left-most header cell that
        # matches any alias, not the alias list's own order.
        path = _write_csv(self.tmp / "two.csv", [
            ["CRN", "Company Number"],
            ["11111111", "22222222"],
        ])
        result = read_company_numbers(path)
        self.assertEqual(result.column, "CRN")
        self.assertEqual(result.numbers, ["11111111"])

    def test_header_row_is_not_treated_as_data(self):
        path = _write_csv(self.tmp / "h.csv", [
            ["Company Number"],
            ["1234567"],
        ])
        result = read_company_numbers(path)
        self.assertEqual(result.numbers, ["01234567"])
        self.assertEqual(result.invalid, [])

    def test_header_below_title_rows_is_found(self):
        # Exported reports often put a title and notes above the table.
        path = _write_xlsx(self.tmp / "report.xlsx", [
            ["UK acquisition pipeline"],
            ["One row per company, merged on Companies House number."],
            [],
            ["ID", "Company", "Companies House no.", "Fit"],
            ["P0000001", "Acme", "01234567", "High"],
            ["P0000002", "Beta", "SC012345", "Low"],
        ])
        result = read_company_numbers(path)
        self.assertEqual(result.column, "Companies House no.")
        self.assertEqual(result.numbers, ["01234567", "SC012345"])
        self.assertEqual(result.invalid, [])

    def test_headerless_file_uses_first_valid_column(self):
        path = _write_csv(self.tmp / "noheader.csv", [
            ["1234567", "Acme"],
            ["SC12345", "Beta"],
            ["bad-one", "Gamma"],
        ])
        result = read_company_numbers(path)
        self.assertIsNone(result.column)
        self.assertEqual(result.numbers, ["01234567", "SC012345"])
        self.assertEqual(result.invalid, ["bad-one"])

    def test_headerless_file_skips_leading_empty_cell(self):
        path = _write_csv(self.tmp / "noheader_gap.csv", [
            ["", "1234567", "Acme"],
            ["", "7654321", "Beta"],
        ])
        result = read_company_numbers(path)
        self.assertIsNone(result.column)
        self.assertEqual(result.numbers, ["01234567", "07654321"])

    def test_unrecognised_header_falls_back_to_first_mostly_valid_column(self):
        path = _write_csv(self.tmp / "fallback.csv", [
            ["Name", "Ref", "Other Ref"],
            ["Acme", "1234567", "11111111"],
            ["Beta", "SC12345", "22222222"],
            ["Gamma", "nope!", "33333333"],
        ])
        result = read_company_numbers(path)
        self.assertEqual(result.column, "Ref")
        self.assertEqual(result.numbers, ["01234567", "SC012345"])
        self.assertEqual(result.invalid, ["nope!"])

    def test_fallback_accepts_column_with_exactly_half_valid(self):
        path = _write_csv(self.tmp / "half.csv", [
            ["Name", "Mixed", "Clean"],
            ["Acme", "1234567", "11111111"],
            ["Beta", "not a number", "22222222"],
        ])
        result = read_company_numbers(path)
        self.assertEqual(result.column, "Mixed")
        self.assertEqual(result.numbers, ["01234567"])
        self.assertEqual(result.invalid, ["not a number"])

    def test_fallback_skips_column_below_half_valid(self):
        path = _write_csv(self.tmp / "below.csv", [
            ["Mostly bad", "Good"],
            ["1234567", "11111111"],
            ["bad one", "22222222"],
            ["bad two", "33333333"],
        ])
        result = read_company_numbers(path)
        self.assertEqual(result.column, "Good")
        self.assertEqual(result.numbers, ["11111111", "22222222", "33333333"])

    def test_no_company_number_column_raises(self):
        path = _write_csv(self.tmp / "none.csv", [
            ["Name", "City"],
            ["Acme Ltd", "London"],
            ["Beta plc", "Leeds"],
        ])
        with self.assertRaises(ValueError):
            read_company_numbers(path)

    def test_empty_cells_skipped_duplicates_and_invalid_deduped_in_order(self):
        path = _write_csv(self.tmp / "dupes.csv", [
            ["Company Number"],
            ["SC12345"],
            [""],
            ["1234567"],
            ["ABC12345"],
            ["sc012345"],      # duplicate of SC12345 after normalisation
            ["  "],
            ["01234567"],      # duplicate of 1234567
            [" ABC12345 "],    # duplicate invalid after stripping
            ["123456789"],
        ])
        result = read_company_numbers(path)
        self.assertEqual(result.numbers, ["SC012345", "01234567"])
        self.assertEqual(result.invalid, ["ABC12345", "123456789"])

    def test_xlsx_numeric_and_text_cells(self):
        path = _write_xlsx(self.tmp / "list.xlsx", [
            ["Company Number", "Name"],
            [1234567, "Acme"],
            [2345678.0, "Beta"],
            ["SC 12345", "Gamma"],
            [None, "Blank"],
            ["oops?", "Delta"],
        ])
        result = read_company_numbers(path)
        self.assertEqual(result.column, "Company Number")
        self.assertEqual(result.numbers, ["01234567", "02345678", "SC012345"])
        self.assertEqual(result.invalid, ["oops?"])

    def test_xlsx_reads_first_worksheet_only(self):
        path = _write_xlsx(
            self.tmp / "multi.xlsx",
            [["CRN"], [11111111]],
            extra_sheet_rows=[["CRN"], [22222222]],
        )
        result = read_company_numbers(path)
        self.assertEqual(result.numbers, ["11111111"])

    def test_xlsx_headerless(self):
        path = _write_xlsx(self.tmp / "noheader.xlsx", [
            [1234567, "Acme"],
            [7654321, "Beta"],
        ])
        result = read_company_numbers(path)
        self.assertIsNone(result.column)
        self.assertEqual(result.numbers, ["01234567", "07654321"])

    def test_unsupported_extension_raises(self):
        for name in ("list.txt", "list.xls", "list.json"):
            with self.subTest(name=name):
                path = self.tmp / name
                path.write_text("Company Number\n01234567\n")
                with self.assertRaises(ValueError):
                    read_company_numbers(path)

    def test_accepts_str_path(self):
        path = _write_csv(self.tmp / "str.csv", [["CRN"], ["1234567"]])
        self.assertEqual(read_company_numbers(str(path)).numbers, ["01234567"])

    def test_csv_with_utf8_bom_matches_header(self):
        # Excel's "CSV UTF-8" export prefixes a BOM.
        path = self.tmp / "bom.csv"
        with path.open("w", newline="", encoding="utf-8-sig") as fh:
            csv.writer(fh).writerows([
                ["Company Number", "Ref"],
                ["1234567", "11111111"],
            ])
        result = read_company_numbers(path)
        self.assertEqual(result.column, "Company Number")
        self.assertEqual(result.numbers, ["01234567"])


if __name__ == "__main__":
    unittest.main()
