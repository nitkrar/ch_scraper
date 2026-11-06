"""Build and write the standard per-company profile workbook for an upload."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
from openpyxl import Workbook
from openpyxl.styles import Font

from ch_bulk.companies_house.company_profile import ProfileLoadResult
from ch_bulk.companies_house.upload_list import UploadInput
from ch_bulk.db.bootstrap import _column_exists, _table_exists
from ch_bulk.db.staging import with_duckdb_connection

PROFILE_COLUMNS: list[str] = [
    "Company Number",
    "Company Name",
    "Incorporation Date",
    "Company Status",
    "Directors",
    "Total Active Directors",
    "Directors 60+",
    "Min Director Age",
    "Avg Director Age",
    "Max Director Age",
    "Employees",
    "Debtors",
    "Current Assets",
    "Creditors Within One Year",
    "Creditors After One Year",
    "Provisions for Liabilities",
    "Deferred Tax",
    "Net Assets",
    "Total Assets",
    "Revenue",
    "Profit Before Tax",
    "Accounts Period End",
    "Accounts Source",
    "Notes",
]
MONEY_COLUMNS = {
    "Debtors",
    "Current Assets",
    "Creditors Within One Year",
    "Creditors After One Year",
    "Provisions for Liabilities",
    "Deferred Tax",
    "Total Assets",
    "Net Assets",
    "Revenue",
    "Profit Before Tax",
}
ACCOUNTS_SOURCE_LABELS = {
    "ixbrl": "Digital accounts",
    "pdf": "PDF",
    "ocr_pdf": "Scanned PDF (OCR)",
}
SHEET_TITLE = "Companies"

NOTE_INVALID = "Invalid company number"
NOTE_NOT_FOUND = "Not found at Companies House"

_ENRICHMENT_COLUMNS = [
    "directors",
    "directors_dob_years",
    "total_active_directors",
    "directors_over_60",
    "min_director_age",
    "avg_director_age",
    "max_director_age",
    "employee_count",
    "debtors",
    "current_assets",
    "creditors_within_one_year",
    "creditors_after_one_year",
    "provisions_for_liabilities",
    "deferred_tax",
    "total_assets",
    "net_assets",
    "revenue",
    "revenue_source",
    "profit_before_tax",
    "filing_period_end",
    "filing_id",
    "filing_format",
]


# Everything a parsed filing can contribute; all blank means nothing was read.
_FILING_FIGURES = (
    "employee_count",
    "debtors",
    "current_assets",
    "creditors_within_one_year",
    "creditors_after_one_year",
    "provisions_for_liabilities",
    "deferred_tax",
    "total_assets",
    "net_assets",
    "revenue",
    "profit_before_tax",
    "filing_period_end",
)


def _empty_row(company_number: str, note: str | None) -> dict[str, object]:
    row: dict[str, object] = dict.fromkeys(PROFILE_COLUMNS)
    row["Company Number"] = company_number
    row["Notes"] = note
    return row


def _format_directors(directors_json: object, current_year: int) -> str | None:
    if directors_json is None:
        return None
    directors = (
        json.loads(directors_json) if isinstance(directors_json, str) else directors_json
    )
    parts: list[str] = []
    for director in directors or []:
        name = director.get("name")
        year = (director.get("date_of_birth") or {}).get("year")
        parts.append(f"{name} ({current_year - int(year)})" if year else f"{name}")
    return "; ".join(parts) or None


def _notes(record: dict[str, object], revenue: object) -> str | None:
    notes: list[str] = []
    if record["directors_dob_years"] is None:
        notes.append("Directors not fetched")
    elif record["total_active_directors"] == 0:
        notes.append("No active directors")
    elif record["avg_director_age"] is None:
        notes.append("No director dates of birth")

    revenue_source = record["revenue_source"]
    if revenue_source == "no_recent_filing":
        notes.append("No accounts filed")
    elif revenue_source == "pdf_no_text_layer":
        notes.append("Scanned PDF accounts - figures pending OCR")
    elif record["filing_id"] is None:
        notes.append("Accounts not fetched")
    elif revenue_source is None and all(record[f] is None for f in _FILING_FIGURES):
        notes.append("Accounts found but could not be read")
    elif record["filing_id"] is not None and revenue is None:
        notes.append("Revenue not in filed accounts")
    return "; ".join(notes) or None


def _profile_row(
    company_number: str,
    record: dict[str, object] | None,
    current_year: int,
) -> dict[str, object]:
    if record is None:
        return _empty_row(company_number, None)
    revenue = (
        None
        if record["revenue_source"] == "employee_band_lookup"
        else record["revenue"]
    )
    return {
        "Company Number": company_number,
        "Company Name": record["company_name"],
        "Incorporation Date": record["incorporation_date"],
        "Company Status": record["company_status"],
        "Directors": _format_directors(record["directors"], current_year),
        "Total Active Directors": record["total_active_directors"],
        "Directors 60+": record["directors_over_60"],
        "Min Director Age": record["min_director_age"],
        "Avg Director Age": record["avg_director_age"],
        "Max Director Age": record["max_director_age"],
        "Employees": record["employee_count"],
        "Debtors": record["debtors"],
        "Current Assets": record["current_assets"],
        "Creditors Within One Year": record["creditors_within_one_year"],
        "Creditors After One Year": record["creditors_after_one_year"],
        "Provisions for Liabilities": record["provisions_for_liabilities"],
        "Deferred Tax": record["deferred_tax"],
        "Net Assets": record["net_assets"],
        "Total Assets": record["total_assets"],
        "Revenue": revenue,
        "Profit Before Tax": record["profit_before_tax"],
        "Accounts Period End": record["filing_period_end"],
        "Accounts Source": ACCOUNTS_SOURCE_LABELS.get(record["filing_format"]),
        "Notes": _notes(record, revenue),
    }


def _load_records(
    con: duckdb.DuckDBPyConnection,
    company_numbers: list[str],
) -> dict[str, dict[str, object]]:
    if not company_numbers:
        return {}
    if _table_exists(con, "company_enrichment"):
        # A database not yet migrated may lack the newer columns.
        enrichment_select = ", ".join(
            f"ce.{c}"
            if _column_exists(con, "company_enrichment", c)
            else f"NULL AS {c}"
            for c in _ENRICHMENT_COLUMNS
        )
        join = "LEFT JOIN company_enrichment ce USING (company_number)"
    else:
        enrichment_select = ", ".join(f"NULL AS {c}" for c in _ENRICHMENT_COLUMNS)
        join = ""
    cursor = con.execute(
        f"""
        SELECT c.company_number, c.company_name, c.incorporation_date,
               c.company_status, {enrichment_select}
        FROM companies c
        {join}
        WHERE c.company_number IN (SELECT UNNEST(?::VARCHAR[]))
        """,
        [company_numbers],
    )
    names = [d[0] for d in cursor.description]
    return {row[0]: dict(zip(names, row)) for row in cursor.fetchall()}


def build_profile_rows(
    db_path: str | Path,
    upload: UploadInput,
    load: ProfileLoadResult,
    *,
    current_year: int,
) -> list[dict[str, object]]:
    not_found = set(load.not_found)
    found = [n for n in upload.numbers if n not in not_found]
    records = with_duckdb_connection(db_path, lambda con: _load_records(con, found))

    rows: list[dict[str, object]] = []
    for number in upload.numbers:
        if number in not_found:
            rows.append(_empty_row(number, NOTE_NOT_FOUND))
        else:
            rows.append(_profile_row(number, records.get(number), current_year))
    rows.extend(_empty_row(raw, NOTE_INVALID) for raw in upload.invalid)
    return rows


def write_profile_xlsx(rows: list[dict[str, object]], path: str | Path) -> Path:
    path = Path(path)
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = SHEET_TITLE
    sheet.append(PROFILE_COLUMNS)
    for cell in sheet[1]:
        cell.font = Font(bold=True)
    sheet.freeze_panes = "A2"

    for row_index, row in enumerate(rows, start=2):
        for col_index, column in enumerate(PROFILE_COLUMNS, start=1):
            value = row.get(column)
            if column == "Company Number" and value is not None:
                value = str(value)
            cell = sheet.cell(row=row_index, column=col_index, value=value)
            if column == "Company Number":
                cell.number_format = "@"
            elif isinstance(value, date):
                cell.number_format = "yyyy-mm-dd"
            elif column in MONEY_COLUMNS:
                cell.number_format = "#,##0"
    workbook.save(path)
    return path


def write_template_xlsx(path: str | Path) -> Path:
    path = Path(path)
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = SHEET_TITLE
    sheet.column_dimensions["A"].number_format = "@"
    sheet["A1"] = "Company Number"
    sheet["A2"] = "01234567"
    sheet["A2"].number_format = "@"
    workbook.save(path)
    return path
