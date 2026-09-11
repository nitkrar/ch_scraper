"""Build a full Companies House export workbook for a list of company numbers.

Takes an input spreadsheet whose first column is a CH company number, joins
everything the local database knows about those companies, and writes a
multi-sheet workbook:

  Summary    one row per company: input columns + headline CH + enrichment
  CH Full    every CH bulk column that carries data for this cohort
  Directors  one row per active director, with birth year and age
  Financials filing-derived figures with their provenance
  Notes      what revenue_source values mean and how to read the numbers

Columns that are entirely empty for the cohort are dropped rather than
exported as dead weight.

Usage:
    python scripts/export_ot_ch_full.py --input <in.xlsx> --output <out.xlsx>
"""
from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import duckdb
import openpyxl
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter

MONEY_COLUMNS = {
    "revenue", "gross_profit", "profit_before_tax", "profit_after_tax",
    "total_assets", "net_assets", "net_current_assets", "fixed_assets",
    "current_assets",
}

ENRICHMENT_COLUMNS = [
    "total_active_directors", "avg_director_age", "min_director_age",
    "max_director_age", "directors_over_60", "all_directors_60_plus",
    "employee_count", "revenue", "revenue_source", "gross_profit",
    "profit_before_tax", "profit_after_tax", "total_assets", "net_assets",
    "net_current_assets", "fixed_assets", "current_assets",
    "filing_period_start", "filing_period_end", "filing_format",
    "filing_age_months", "last_enriched_at",
]

SUMMARY_CH = [
    "company_name", "company_status", "company_type", "incorporation_date",
    "sic_code_1", "sic_text_1", "registered_address", "postcode",
    "address_post_town", "address_county", "accounts_category",
    "accounts_last_made_up", "accounts_next_due", "conf_stmt_next_due",
    "uri",
]
SUMMARY_ENRICH = [
    "total_active_directors", "avg_director_age", "min_director_age",
    "max_director_age", "directors_over_60", "employee_count", "revenue",
    "revenue_source", "profit_before_tax", "profit_after_tax", "net_assets",
    "total_assets", "filing_period_end",
]

NOTES = [
    ("revenue_source", "How the revenue figure was obtained. Read this before using revenue."),
    ("  filed_accounts_ixbrl", "Turnover tagged in the company's own iXBRL accounts. Reported figure."),
    ("  filed_accounts_pdf_ocr", "Recovered by OCR from a scanned PDF filing, then reconciled by hand."),
    ("  employee_band_lookup", "ESTIMATE. Derived from employee count via a revenue band, not reported."),
    ("  partial_no_revenue", "Accounts filed but no turnover disclosed (micro-entity / FRS 105)."),
    ("  no_recent_filing", "No recent annual accounts on file."),
    ("", ""),
    ("Balance sheet figures", "As filed. More reliable than revenue for this cohort."),
    ("Director ages", "Companies House publishes birth month/year only; age is year-based."),
    ("Employee count", "Average for the period, as disclosed in the accounts."),
]


def _fmt(value):
    """Coerce a DB value into something openpyxl will accept."""
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if value is None or isinstance(value, (int, float, bool, str)):
        return value
    return str(value)


def _loads(value):
    if isinstance(value, str):
        return json.loads(value)
    return value or []


def _style(ws, header, widths_from_header=True):
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(vertical="top", wrap_text=False)
    ws.freeze_panes = "B2"
    ws.auto_filter.ref = ws.dimensions
    for idx, name in enumerate(header, 1):
        width = min(max(len(str(name)) + 2, 12), 32) if widths_from_header else 18
        ws.column_dimensions[get_column_letter(idx)].width = width
    money_idx = [i for i, h in enumerate(header, 1) if h in MONEY_COLUMNS]
    for col in money_idx:
        letter = get_column_letter(col)
        for row in range(2, ws.max_row + 1):
            ws[f"{letter}{row}"].number_format = "#,##0.00"


def build(input_path: Path, output_path: Path, db_path: Path) -> None:
    ws_in = openpyxl.load_workbook(input_path, data_only=True).worksheets[0]
    rows_in = list(ws_in.iter_rows(min_row=1, values_only=True))
    in_hdr = [str(h) for h in rows_in[0]]
    in_data = [r for r in rows_in[1:] if r[0]]
    order = [str(r[0]).strip() for r in in_data]

    con = duckdb.connect(str(db_path), read_only=True)
    con.execute("CREATE TEMP TABLE ot(company_number VARCHAR)")
    con.executemany("INSERT INTO ot VALUES (?)", [(n,) for n in order])

    # Keep only CH columns with at least one value across the cohort.
    all_ch = [c[0] for c in con.execute("DESCRIBE companies").fetchall()]
    counts = con.execute(
        "SELECT " + ", ".join(f'COUNT("{c}")' for c in all_ch)
        + " FROM ot JOIN companies USING (company_number)"
    ).fetchone()
    ch_live = [c for c, n in zip(all_ch, counts) if n and c != "company_number"]

    ch_sel = ", ".join(f'"{c}"' for c in ch_live)
    ch = {
        r[0]: dict(zip(ch_live, r[1:]))
        for r in con.execute(
            f"SELECT company_number, {ch_sel} FROM ot JOIN companies USING (company_number)"
        ).fetchall()
    }

    enr_sel = ", ".join(f'"{c}"' for c in ENRICHMENT_COLUMNS)
    enr = {
        r[0]: dict(zip(ENRICHMENT_COLUMNS, r[1:]))
        for r in con.execute(
            f"SELECT company_number, {enr_sel} FROM ot JOIN company_enrichment USING (company_number)"
        ).fetchall()
    }

    directors = {
        r[0]: (_loads(r[1]), _loads(r[2]))
        for r in con.execute(
            "SELECT company_number, directors, directors_dob_years "
            "FROM ot JOIN company_enrichment USING (company_number)"
        ).fetchall()
    }
    con.close()

    wb = openpyxl.Workbook()

    # ---- Summary ----
    ws = wb.active
    ws.title = "Summary"
    header = in_hdr + SUMMARY_CH + SUMMARY_ENRICH
    ws.append(header)
    for row in in_data:
        cn = str(row[0]).strip()
        c, e = ch.get(cn, {}), enr.get(cn, {})
        ws.append(list(row)
                  + [_fmt(c.get(k)) for k in SUMMARY_CH]
                  + [_fmt(e.get(k)) for k in SUMMARY_ENRICH])
    _style(ws, header)

    # ---- CH Full ----
    ws = wb.create_sheet("CH Full")
    header = ["company_number"] + ch_live
    ws.append(header)
    for cn in order:
        c = ch.get(cn, {})
        ws.append([cn] + [_fmt(c.get(k)) for k in ch_live])
    _style(ws, header)

    # ---- Directors ----
    ws = wb.create_sheet("Directors")
    age_year = date.today().year
    header = ["company_number", "company_name", "director_name", "officer_role",
              "appointed_on", "birth_year", f"age_{age_year}"]
    ws.append(header)
    for cn in order:
        people, years = directors.get(cn, ([], []))
        # Birth years are only positionally comparable when every active
        # director had a DOB; otherwise leave the column blank rather than
        # attach the wrong year to a name.
        aligned = len(people) == len(years)
        for i, person in enumerate(people):
            year = years[i] if aligned and i < len(years) else None
            ws.append([
                cn,
                ch.get(cn, {}).get("company_name"),
                person.get("name"),
                person.get("officer_role"),
                person.get("appointed_on"),
                year,
                (age_year - year) if year else None,
            ])
    _style(ws, header)

    # ---- Financials ----
    ws = wb.create_sheet("Financials")
    fin = ["employee_count", "revenue", "revenue_source", "gross_profit",
           "profit_before_tax", "profit_after_tax", "total_assets", "net_assets",
           "net_current_assets", "fixed_assets", "current_assets",
           "filing_period_start", "filing_period_end", "filing_format",
           "filing_age_months", "last_enriched_at"]
    header = ["company_number", "company_name"] + fin
    ws.append(header)
    for cn in order:
        e = enr.get(cn, {})
        ws.append([cn, ch.get(cn, {}).get("company_name")] + [_fmt(e.get(k)) for k in fin])
    _style(ws, header)

    # ---- Notes ----
    ws = wb.create_sheet("Notes")
    ws.append(["Field", "Meaning"])
    for a, b in NOTES:
        ws.append([a, b])
    for cell in ws[1]:
        cell.font = Font(bold=True)
    ws.column_dimensions["A"].width = 26
    ws.column_dimensions["B"].width = 96

    wb.save(output_path)
    return {
        "companies": len(order),
        "ch_columns": len(ch_live),
        "directors": ws_count if (ws_count := sum(len(directors.get(c, ([], []))[0]) for c in order)) else 0,
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--db-path", type=Path, default=Path("data/db/ch_bulk.duckdb"))
    args = p.parse_args()
    stats = build(args.input, args.output, args.db_path)
    print(f"wrote {args.output}")
    print(f"  companies={stats['companies']} ch_columns={stats['ch_columns']} "
          f"director_rows={stats['directors']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
