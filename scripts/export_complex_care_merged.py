"""Merge the "New Complex care" screened locations into one row per business.

Answers Heena's four asks against the local CH + CQC database:

  A. Merge locations belonging to the same business into a single row,
     keyed on Provider Companies House Number (falling back to Provider ID
     for the handful of rows CQC holds no company number for).
  B. Attach director age, director count, employee count and revenue.
  C. Cross-check the "Selected 56" tab against the screened set and flag
     the matches as Tier 1.
  D. Flag as Tier 1 any business with a director aged 58+, incorporated
     more than 5 years ago, and more than 20 employees.

The 58+ test reuses the director-age logic already used for the "Over 60"
export flag (ch_enricher.compute_age_fields): count active directors whose
age is >= the threshold, flag when at least one qualifies.

Run: python scripts/export_complex_care_merged.py
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

import duckdb
import openpyxl

sys.path.insert(0, str(Path(__file__).resolve().parent))

from export_homecare_xlsx import (  # noqa: E402
    HEADER_FILL,
    HEADER_FONT,
    TIER1_FILL,
    SECTION_FONT,
    style_tier_column,
    write_table,
)

from ch_bulk.companies_house.financials_parsers import coerce_employee_count  # noqa: E402
from ch_bulk.companies_house.revenue_model import estimate, load_bands  # noqa: E402
from ch_bulk.core.paths import DEFAULT_DATA_DIR, DEFAULT_DB_PATH, revenue_bands_path  # noqa: E402

SOURCE_XLSX = Path("/Users/nitinkum/Downloads/New Complex care_Claude.xlsx")

# Heena's part-D thresholds.
AGE_THRESHOLD = 58
MIN_YEARS_TRADING = 5
MIN_EMPLOYEES = 20

# Revenue written by the employee-band model rather than read off a filing.
ESTIMATED_REVENUE_SOURCES = {"employee_band_lookup"}

SERVICE_USER_BAND_COLS = [
    "Service user band - Dementia",
    "Service user band - Learning disabilities or autistic spectrum disorder",
    "Service user band - Mental Health",
    "Service user band - Older People",
    "Service user band - Younger Adults",
    "Service user band - Children 0-18 years",
]


# ----------------------------- helpers -----------------------------------


def norm_company_number(value) -> str | None:
    """Normalise a CH number cell to the 8-char form used in the database."""
    if value is None:
        return None
    if isinstance(value, float) and value != value:  # NaN
        return None
    text = str(value).strip().upper().replace(" ", "")
    if text.endswith(".0"):
        text = text[:-2]
    if not text or text == "NAN":
        return None
    return text.zfill(8) if text.isdigit() else text


def is_yes(value) -> bool:
    return str(value).strip().upper() == "Y"


def first_non_empty(values):
    for v in values:
        if v is None:
            continue
        if isinstance(v, float) and v != v:
            continue
        text = str(v).strip()
        if text and text.upper() != "NAN":
            return text
    return None


def distinct_joined(values, limit: int = 6) -> str | None:
    seen: list[str] = []
    for v in values:
        text = first_non_empty([v])
        if text and text not in seen:
            seen.append(text)
    if not seen:
        return None
    if len(seen) > limit:
        return "; ".join(seen[:limit]) + f" (+{len(seen) - limit} more)"
    return "; ".join(seen)


def read_sheet(path: Path, name: str) -> tuple[list[str], list[dict]]:
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb[name]
    rows = ws.iter_rows(values_only=True)
    headers = [str(h).strip() if h is not None else "" for h in next(rows)]
    out = []
    for r in rows:
        if r is None or all(c is None for c in r):
            continue
        out.append(dict(zip(headers, r)))
    wb.close()
    return headers, out


def years_between(start: date | None, end: date) -> float | None:
    if start is None:
        return None
    return (end - start).days / 365.25


def usable_employee_count(raw) -> tuple[int | None, object]:
    """Return (usable count, rejected raw value).

    Rows enriched before the parser gained a plausibility bound can still hold
    a mistagged monetary figure (e.g. 1,760,252 "employees"). Those would sail
    through the >20-employee test, so drop them here rather than tier on them.
    """
    usable = coerce_employee_count(raw)
    return usable, (raw if raw is not None and usable is None else None)


def directors_at_or_over(dob_years_json, current_year: int, threshold: int):
    """Mirror ch_enricher.compute_age_fields, at an arbitrary age threshold."""
    if not dob_years_json:
        return None, None
    try:
        years = json.loads(dob_years_json)
    except (TypeError, ValueError):
        return None, None
    ages = [current_year - int(y) for y in years if y is not None]
    if not ages:
        return None, None
    return (
        sum(1 for age in ages if age >= threshold),
        all(age >= threshold for age in ages),
    )


# ------------------------- database lookup --------------------------------


COMPANY_SQL = """
SELECT
    c.company_number,
    c.company_name,
    c.company_status,
    c.company_type,
    c.incorporation_date,
    c.accounts_category,
    c.postcode,
    e.avg_director_age,
    e.min_director_age,
    e.max_director_age,
    e.directors_over_60,
    e.directors_dob_years,
    e.total_active_directors,
    e.employee_count,
    e.revenue,
    e.revenue_source,
    e.net_assets,
    e.total_assets,
    e.profit_before_tax,
    e.filing_period_end
FROM companies c
LEFT JOIN company_enrichment e USING (company_number)
WHERE c.company_number IN (SELECT chn FROM wanted)
"""


def load_company_facts(con, numbers: list[str]) -> dict[str, dict]:
    con.execute("CREATE OR REPLACE TEMP TABLE wanted (chn VARCHAR)")
    con.executemany("INSERT INTO wanted VALUES (?)", [(n,) for n in numbers])
    cur = con.execute(COMPANY_SQL)
    cols = [d[0] for d in cur.description]
    return {row[0]: dict(zip(cols, row)) for row in cur.fetchall()}


def load_hsca_presence(con, numbers: list[str]) -> dict[str, dict]:
    """Location counts from the current HSCA file, for companies off the screen."""
    con.execute("CREATE OR REPLACE TEMP TABLE wanted2 (chn VARCHAR)")
    con.executemany("INSERT INTO wanted2 VALUES (?)", [(n,) for n in numbers])
    cur = con.execute(
        """
        SELECT
            l.provider_companies_house_number AS chn,
            COUNT(*) AS locations,
            COUNT(*) FILTER (
                WHERE l.regulated_activities::VARCHAR
                      ILIKE '%disease, disorder or injury%'
            ) AS tddi_locations
        FROM cqc_hsca_locations l
        JOIN wanted2 w ON w.chn = l.provider_companies_house_number
        GROUP BY 1
        """
    )
    cols = [d[0] for d in cur.description]
    return {row[0]: dict(zip(cols, row)) for row in cur.fetchall()}


# --------------------------- row assembly ---------------------------------


def build_business_row(key, group, facts, bands, today, current_year, in_selected_56):
    ch_number = group[0]["_chn"]
    company = facts.get(ch_number, {}) if ch_number else {}

    incorporation = company.get("incorporation_date")
    age_years = years_between(incorporation, today)
    over_58, all_58 = directors_at_or_over(
        company.get("directors_dob_years"), current_year, AGE_THRESHOLD
    )

    revenue = company.get("revenue")
    revenue_source = company.get("revenue_source")
    filed_revenue = (
        revenue if revenue is not None and revenue_source not in ESTIMATED_REVENUE_SOURCES else None
    )
    employees, rejected_employees = usable_employee_count(company.get("employee_count"))
    estimated_revenue = estimate(employees, bands)

    # Part D: director 58+, trading > 5 years, more than 20 employees.
    d_age = bool(over_58)
    d_years = age_years is not None and age_years > MIN_YEARS_TRADING
    d_size = employees is not None and employees > MIN_EMPLOYEES
    meets_d = d_age and d_years and d_size

    reasons = []
    if in_selected_56:
        reasons.append("In Selected 56")
    if meets_d:
        reasons.append(
            f"Director {AGE_THRESHOLD}+, >{MIN_YEARS_TRADING}y old, >{MIN_EMPLOYEES} employees"
        )

    # Why part D could not be evaluated, so blanks aren't read as failures.
    missing = []
    if over_58 is None:
        missing.append("director ages")
    if age_years is None:
        missing.append("incorporation date")
    if employees is None:
        missing.append("employee count")

    return {
        "Companies House Number": ch_number,
        "Provider Name (CQC)": first_non_empty(r.get("Provider Name") for r in group),
        "Company Name (Companies House)": company.get("company_name"),
        "Company Status": company.get("company_status"),
        "Provider ID": first_non_empty(r.get("Provider ID") for r in group),
        "Locations Merged": len(group),
        "Location Names": distinct_joined(r.get("Location Name") for r in group),
        "Location IDs": distinct_joined((r.get("Location ID") for r in group), limit=10),
        "Regions": distinct_joined(r.get("Location Region") for r in group),
        "Local Authorities": distinct_joined(r.get("Location Local Authority") for r in group),
        "Provider City": first_non_empty(r.get("Provider City") for r in group),
        "Provider Postcode": first_non_empty(r.get("Provider Postal Code") for r in group),
        "Provider Web Address": first_non_empty(r.get("Provider Web Address") for r in group),
        "Provider Telephone": first_non_empty(r.get("Provider Telephone Number") for r in group),
        "Nominated Individual": first_non_empty(
            r.get("Provider Nominated Individual Name") for r in group
        ),
        "Ratings": distinct_joined(r.get("Location Latest Overall Rating") for r in group),
        "Any Supported Living": any(
            is_yes(r.get("Service type - Supported living service")) for r in group
        ),
        "Earliest HSCA Start": min(
            (r.get("Location HSCA start date") for r in group if r.get("Location HSCA start date")),
            default=None,
        ),
        # ---- Companies House facts (part B) ----
        "Incorporation Date": incorporation,
        "Company Age (years)": round(age_years, 1) if age_years is not None else None,
        "Trading > 5 Years": d_years if age_years is not None else None,
        "Director Count": company.get("total_active_directors"),
        "Avg Director Age": company.get("avg_director_age"),
        "Oldest Director Age": company.get("max_director_age"),
        "Youngest Director Age": company.get("min_director_age"),
        f"Directors {AGE_THRESHOLD}+": over_58,
        f"All Directors {AGE_THRESHOLD}+": all_58,
        "Directors 60+": company.get("directors_over_60"),
        "Employee Count": employees,
        "Employee Count Rejected (implausible)": rejected_employees,
        "Revenue (filed accounts)": filed_revenue,
        "Revenue Source": revenue_source,
        "Revenue (estimated from employees)": estimated_revenue,
        "Net Assets": company.get("net_assets"),
        "Total Assets": company.get("total_assets"),
        "Profit Before Tax": company.get("profit_before_tax"),
        "Accounts Category": company.get("accounts_category"),
        "Latest Filing Period End": company.get("filing_period_end"),
        # ---- Tiering (parts C and D) ----
        "Tier": "Tier 1" if reasons else None,
        "Tier 1 Reason": "; ".join(reasons) if reasons else None,
        "In Selected 56": in_selected_56,
        "Meets Part D Criteria": meets_d,
        "Part D Not Assessable": ", ".join(missing) if missing else None,
        "Source": "Screened tab",
    }


def load_hsca_file_date(con) -> date | None:
    """The publish date of the HSCA snapshot currently loaded."""
    row = con.execute("SELECT MAX(bulk_file_date) FROM cqc_hsca_locations").fetchone()
    return row[0] if row else None


def build_off_screen_row(
    ch_number, sel_row, facts, hsca, bands, today, current_year, hsca_file_label
):
    """A "Selected 56" company that is not in the screened set."""
    company = facts.get(ch_number, {})
    presence = hsca.get(ch_number)

    if presence is None:
        reason = f"Not in CQC HSCA active locations ({hsca_file_label} file)"
    elif not presence["tddi_locations"]:
        reason = (
            f"In CQC register ({presence['locations']} location(s)) "
            "but no TDDI regulated activity"
        )
    else:
        reason = "Holds TDDI but outside the screened service types"

    incorporation = company.get("incorporation_date")
    age_years = years_between(incorporation, today)
    over_58, all_58 = directors_at_or_over(
        company.get("directors_dob_years"), current_year, AGE_THRESHOLD
    )
    revenue = company.get("revenue")
    revenue_source = company.get("revenue_source")
    filed_revenue = (
        revenue if revenue is not None and revenue_source not in ESTIMATED_REVENUE_SOURCES else None
    )
    employees, rejected_employees = usable_employee_count(company.get("employee_count"))

    meets_d = (
        bool(over_58)
        and age_years is not None
        and age_years > MIN_YEARS_TRADING
        and employees is not None
        and employees > MIN_EMPLOYEES
    )
    reasons = ["In Selected 56"]
    if meets_d:
        reasons.append(
            f"Director {AGE_THRESHOLD}+, >{MIN_YEARS_TRADING}y old, >{MIN_EMPLOYEES} employees"
        )

    return {
        "Companies House Number": ch_number,
        "Provider Name (CQC)": None,
        "Company Name (Companies House)": company.get("company_name") or sel_row.get("Company"),
        "Company Status": company.get("company_status"),
        "Provider ID": None,
        "Locations Merged": presence["locations"] if presence else 0,
        "Location Names": None,
        "Location IDs": None,
        "Regions": None,
        "Local Authorities": None,
        "Provider City": None,
        "Provider Postcode": company.get("postcode"),
        "Provider Web Address": sel_row.get("Website"),
        "Provider Telephone": sel_row.get("General Phone"),
        "Nominated Individual": None,
        "Ratings": None,
        "Any Supported Living": None,
        "Earliest HSCA Start": None,
        "Incorporation Date": incorporation,
        "Company Age (years)": round(age_years, 1) if age_years is not None else None,
        "Trading > 5 Years": (age_years > MIN_YEARS_TRADING) if age_years is not None else None,
        "Director Count": company.get("total_active_directors"),
        "Avg Director Age": company.get("avg_director_age"),
        "Oldest Director Age": company.get("max_director_age"),
        "Youngest Director Age": company.get("min_director_age"),
        f"Directors {AGE_THRESHOLD}+": over_58,
        f"All Directors {AGE_THRESHOLD}+": all_58,
        "Directors 60+": company.get("directors_over_60"),
        "Employee Count": employees,
        "Employee Count Rejected (implausible)": rejected_employees,
        "Revenue (filed accounts)": filed_revenue,
        "Revenue Source": revenue_source,
        "Revenue (estimated from employees)": estimate(employees, bands),
        "Net Assets": company.get("net_assets"),
        "Total Assets": company.get("total_assets"),
        "Profit Before Tax": company.get("profit_before_tax"),
        "Accounts Category": company.get("accounts_category"),
        "Latest Filing Period End": company.get("filing_period_end"),
        "Tier": "Tier 1",
        "Tier 1 Reason": "; ".join(reasons),
        "In Selected 56": True,
        "Meets Part D Criteria": meets_d,
        "Part D Not Assessable": None,
        "Source": f"Selected 56 only - {reason}",
    }


# ------------------------------ output ------------------------------------

MONEY_COLS = (
    "Revenue (filed accounts)",
    "Revenue (estimated from employees)",
    "Net Assets",
    "Total Assets",
    "Profit Before Tax",
)
DATE_COLS = ("Incorporation Date", "Latest Filing Period End", "Earliest HSCA Start")
BOOL_COLS = (
    "Any Supported Living",
    "Trading > 5 Years",
    f"All Directors {AGE_THRESHOLD}+",
    "In Selected 56",
    "Meets Part D Criteria",
)


def write_summary(ws, stats: list[tuple[str, object]], notes: list[str]):
    ws.append(["New Complex Care - merged business view"])
    ws["A1"].font = SECTION_FONT
    ws.append([])
    ws.append(["Metric", "Value"])
    for cell in ws[3]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
    for label, value in stats:
        ws.append([label, value])
    ws.append([])
    row = ws.max_row + 1
    ws.cell(row=row, column=1, value="Notes").font = SECTION_FONT
    for note in notes:
        ws.append([note])
    ws.column_dimensions["A"].width = 62
    ws.column_dimensions["B"].width = 22


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=SOURCE_XLSX)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    today = date.today()
    current_year = today.year
    bands = load_bands(revenue_bands_path(args.data_dir))

    _, screened = read_sheet(args.source, "Screened")
    _, selected = read_sheet(args.source, "Selected 56")

    for row in screened:
        row["_chn"] = norm_company_number(row.get("Provider Companies House Number"))

    selected_by_number: dict[str, dict] = {}
    for row in selected:
        num = norm_company_number(row.get("Company No."))
        if num:
            selected_by_number[num] = row

    # Part A: one row per business. CH number is the merge key; fall back to
    # Provider ID for rows CQC holds no company number against.
    groups: dict[tuple[str, str], list[dict]] = {}
    for row in screened:
        key = ("chn", row["_chn"]) if row["_chn"] else ("provider", str(row.get("Provider ID")))
        groups.setdefault(key, []).append(row)

    screened_numbers = sorted({r["_chn"] for r in screened if r["_chn"]})
    all_numbers = sorted(set(screened_numbers) | set(selected_by_number))

    con = duckdb.connect(str(args.db_path), read_only=True)
    facts = load_company_facts(con, all_numbers)
    off_screen = sorted(set(selected_by_number) - set(screened_numbers))
    hsca = load_hsca_presence(con, off_screen) if off_screen else {}
    hsca_file_date = load_hsca_file_date(con)
    con.close()
    hsca_file_label = hsca_file_date.strftime("%d %B %Y") if hsca_file_date else "current"

    businesses = [
        build_business_row(
            key,
            group,
            facts,
            bands,
            today,
            current_year,
            in_selected_56=bool(group[0]["_chn"] and group[0]["_chn"] in selected_by_number),
        )
        for key, group in groups.items()
    ]
    businesses.extend(
        build_off_screen_row(
            num,
            selected_by_number[num],
            facts,
            hsca,
            bands,
            today,
            current_year,
            hsca_file_label,
        )
        for num in off_screen
    )

    businesses.sort(
        key=lambda b: (
            b["Tier"] != "Tier 1",
            -(b["Employee Count"] or 0),
            b["Company Name (Companies House)"] or b["Provider Name (CQC)"] or "",
        )
    )

    headers = list(businesses[0].keys())
    rows = [tuple(b[h] for h in headers) for b in businesses]
    tier1 = [r for b, r in zip(businesses, rows) if b["Tier"] == "Tier 1"]

    # Location-level detail, with the tier decision carried down.
    tier_by_number = {b["Companies House Number"]: b["Tier"] for b in businesses}
    loc_headers = [
        "Location ID",
        "Location Name",
        "Provider Name",
        "Companies House Number",
        "Tier",
        "Location Region",
        "Location Local Authority",
        "Location Latest Overall Rating",
    ]
    loc_rows = [
        (
            r.get("Location ID"),
            r.get("Location Name"),
            r.get("Provider Name"),
            r["_chn"],
            tier_by_number.get(r["_chn"]),
            r.get("Location Region"),
            r.get("Location Local Authority"),
            r.get("Location Latest Overall Rating"),
        )
        for r in screened
    ]

    # Selected 56 cross-check (part C).
    by_number = {b["Companies House Number"]: b for b in businesses}
    check_headers = [
        "Company (Selected 56)",
        "Company No.",
        "In Screened Tab",
        "Matched Business Row",
        "Why Not In Screened",
        "Tier",
    ]
    check_rows = []
    for num, sel_row in sorted(selected_by_number.items(), key=lambda kv: str(kv[1].get("Company"))):
        biz = by_number.get(num)
        in_screened = num in screened_numbers
        check_rows.append(
            (
                sel_row.get("Company"),
                num,
                "Yes" if in_screened else "No",
                (biz or {}).get("Company Name (Companies House)"),
                None if in_screened else (biz or {}).get("Source", "").replace("Selected 56 only - ", ""),
                (biz or {}).get("Tier"),
            )
        )

    n_screened_biz = len(groups)
    n_tier1 = len(tier1)
    n_from_56 = sum(1 for b in businesses if b["In Selected 56"])
    n_from_d = sum(1 for b in businesses if b["Meets Part D Criteria"])
    n_filed_rev = sum(1 for b in businesses if b["Revenue (filed accounts)"] is not None)
    n_emp = sum(1 for b in businesses if b["Employee Count"] is not None)
    n_dir_age = sum(1 for b in businesses if b[f"Directors {AGE_THRESHOLD}+"] is not None)
    n_rejected_emp = sum(
        1 for b in businesses if b["Employee Count Rejected (implausible)"] is not None
    )

    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    write_summary(
        wb.create_sheet("Summary"),
        [
            ("Screened locations (input)", len(screened)),
            ("Distinct businesses after merge (part A)", n_screened_biz),
            ("Selected 56 companies appended (not in screened)", len(off_screen)),
            ("Total business rows", len(businesses)),
            ("", ""),
            ("Tier 1 total", n_tier1),
            ("  - via Selected 56 (part C)", n_from_56),
            ("  - via part D criteria", n_from_d),
            ("", ""),
            ("Selected 56 found in screened tab", 56 - len(off_screen)),
            ("Selected 56 NOT in screened tab", len(off_screen)),
            ("", ""),
            ("Businesses with director ages", n_dir_age),
            ("Businesses with employee count", n_emp),
            ("Businesses with filed revenue", n_filed_rev),
            ("Employee counts rejected as implausible", n_rejected_emp),
        ],
        [
            f"Generated {today.isoformat()} from {args.source.name}.",
            f"CQC source: HSCA Active Locations, {hsca_file_label} file (latest published).",
            "Part A: merged on Provider Companies House Number; Provider ID used where CQC holds no company number.",
            f"Part D: at least one active director aged {AGE_THRESHOLD}+, incorporated more than "
            f"{MIN_YEARS_TRADING} years ago, and more than {MIN_EMPLOYEES} employees.",
            f"The {AGE_THRESHOLD}+ test uses the same rule as the existing 'Over 60' export flag: "
            f"count of active directors whose age is {AGE_THRESHOLD} or above.",
            "Revenue (filed accounts) is only populated where turnover actually appears in a filing.",
            "Most of these companies file micro-entity or total-exemption accounts, where turnover is "
            "legally not disclosed, so this column is sparse by nature - it is not a scraping gap.",
            "Revenue (estimated from employees) is a modelled figure from data/reference/revenue_bands.csv. "
            "It is an indicative size band for ranking only, not a reported number.",
            "A few filings tag a monetary figure against the employee-count concept. Those values are "
            "rejected rather than tiered on, and the discarded figure is kept in "
            "'Employee Count Rejected (implausible)' so the row can be checked by hand.",
        ],
    )

    ws = wb.create_sheet("Merged Businesses")
    write_table(ws, headers, rows, money_cols=MONEY_COLS, date_cols=DATE_COLS, bool_cols=BOOL_COLS)
    style_tier_column(ws)

    ws = wb.create_sheet("Tier 1")
    write_table(ws, headers, tier1, money_cols=MONEY_COLS, date_cols=DATE_COLS, bool_cols=BOOL_COLS)
    style_tier_column(ws)

    ws = wb.create_sheet("Selected 56 Check")
    write_table(ws, check_headers, check_rows)
    style_tier_column(ws)

    ws = wb.create_sheet("Screened Locations")
    write_table(ws, loc_headers, loc_rows)
    style_tier_column(ws)

    output = args.output or (
        Path(args.data_dir) / "exports" / f"complex_care_merged_{today:%Y%m%d}.xlsx"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output)
    print(f"Wrote {output}")
    print(
        f"  {len(screened)} locations -> {n_screened_biz} businesses "
        f"(+{len(off_screen)} from Selected 56) | Tier 1: {n_tier1}"
    )


if __name__ == "__main__":
    main()
