"""iXBRL and PDF parser helpers for financial filing enrichment."""

from __future__ import annotations

import io
import re
from datetime import date
from typing import Any

import pdfplumber
from ixbrlparse import IXBRL

from ch_bulk.companies_house.financials_contracts import ParsedFinancialFacts

REVENUE_FACTS = (
    "TurnoverRevenue",
    "Revenue",
    "TurnoverGrossOperatingRevenue",
)
EMPLOYEE_COUNT_FACTS = (
    "AverageNumberEmployeesDuringPeriod",
    "AverageNumberEmployeesDuringYear",
)
# These facts are a headcount, and two things put non-headcount values in them:
#   - the OCR path reading the staff-costs total out of the "Employees and
#     directors" note (wages + NI + pension) instead of the headcount below it;
#   - filings that tag the headcount with sign="-", which the iXBRL spec says
#     to negate, giving "-9 employees".
# Neither is recoverable here, so bound the value instead. The PDF regex path
# already caps its match at six digits; hold every path to the same ceiling.
MAX_PLAUSIBLE_EMPLOYEE_COUNT = 999_999
GROSS_PROFIT_FACTS = ("GrossProfitLoss",)
PROFIT_BEFORE_TAX_FACTS = (
    "ProfitLossOnOrdinaryActivitiesBeforeTax",
    "ProfitLossBeforeTax",
)
PROFIT_AFTER_TAX_FACTS = ("ProfitLossOnOrdinaryActivitiesAfterTax",)
FIXED_ASSETS_FACTS = (
    "FixedAssets",
    "PropertyPlantEquipment",
)
CURRENT_ASSETS_FACTS = ("CurrentAssets",)
NET_ASSETS_FACTS = (
    "NetAssetsLiabilities",
    "NetAssetsLiabilitiesIncludingPensionAssetLiability",
    "TotalAssetsLessCurrentLiabilities",
)
NET_CURRENT_ASSETS_FACTS = ("NetCurrentAssetsLiabilities",)
PERIOD_FACT_GROUPS = (
    REVENUE_FACTS,
    GROSS_PROFIT_FACTS,
    PROFIT_BEFORE_TAX_FACTS,
    PROFIT_AFTER_TAX_FACTS,
    EMPLOYEE_COUNT_FACTS,
)
IXBRL_PROFIT_LOSS_EXEMPTION_PATTERNS = (
    re.compile(
        r"statementthatdirectorshaveelectednottodeliverprofitlossaccount",
        re.IGNORECASE,
    ),
    re.compile(
        r"profit\s*(?:and|&)?\s*loss\s+account\s+has\s+not\s+been\s+delivered",
        re.IGNORECASE,
    ),
    re.compile(
        r"statement\s+of\s+income(?:\s+and\s+retained\s+earnings)?\s+has\s+not\s+been\s+delivered",
        re.IGNORECASE,
    ),
    re.compile(r"small\s+companies\s+regime", re.IGNORECASE),
    re.compile(r"section\s*444(?:5a)?", re.IGNORECASE),
)


def _empty_parsed_financials(
    *,
    parse_status: str,
    parse_failure_reason: str | None,
) -> ParsedFinancialFacts:
    return ParsedFinancialFacts(
        revenue=None,
        employee_count=None,
        filing_period_start=None,
        filing_period_end=None,
        gross_profit=None,
        profit_before_tax=None,
        profit_after_tax=None,
        fixed_assets=None,
        current_assets=None,
        total_assets=None,
        net_assets=None,
        net_current_assets=None,
        parse_status=parse_status,
        parse_failure_reason=parse_failure_reason,
        profit_loss_exempt=False,
    )


def _missing_financial_reasons(
    *,
    revenue: float | None,
    employee_count: int | None,
    gross_profit: float | None,
    profit_before_tax: float | None,
    profit_after_tax: float | None,
    fixed_assets: float | None,
    current_assets: float | None,
    total_assets: float | None,
    net_assets: float | None,
    net_current_assets: float | None,
) -> str | None:
    missing: list[str] = []
    if revenue is None:
        missing.append("revenue_missing")
    if employee_count is None:
        missing.append("employee_count_missing")
    if gross_profit is None:
        missing.append("gross_profit_missing")
    if profit_before_tax is None:
        missing.append("profit_before_tax_missing")
    if profit_after_tax is None:
        missing.append("profit_after_tax_missing")
    if fixed_assets is None:
        missing.append("fixed_assets_missing")
    if current_assets is None:
        missing.append("current_assets_missing")
    if total_assets is None:
        missing.append("total_assets_missing")
    if net_assets is None:
        missing.append("net_assets_missing")
    if net_current_assets is None:
        missing.append("net_current_assets_missing")
    return ",".join(missing) if missing else None


def _classify_financial_facts(
    *,
    revenue: float | None,
    employee_count: int | None,
    gross_profit: float | None,
    profit_before_tax: float | None,
    profit_after_tax: float | None,
    fixed_assets: float | None,
    current_assets: float | None,
    total_assets: float | None,
    net_assets: float | None,
    net_current_assets: float | None,
    partial_status: str,
    error_status: str,
) -> tuple[str, str | None]:
    if revenue is not None and employee_count is not None:
        return "ok", None

    if any(
        value is not None
        for value in (
            revenue,
            employee_count,
            gross_profit,
            profit_before_tax,
            profit_after_tax,
            fixed_assets,
            current_assets,
            total_assets,
            net_assets,
            net_current_assets,
        )
    ):
        return (
            partial_status,
            _missing_financial_reasons(
                revenue=revenue,
                employee_count=employee_count,
                gross_profit=gross_profit,
                profit_before_tax=profit_before_tax,
                profit_after_tax=profit_after_tax,
                fixed_assets=fixed_assets,
                current_assets=current_assets,
                total_assets=total_assets,
                net_assets=net_assets,
                net_current_assets=net_current_assets,
            ),
        )

    return error_status, "no_supported_facts_found"


def _parse_date(value: object) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def coerce_employee_count(value: object) -> int | None:
    """Coerce an extracted employee headcount, dropping implausible values.

    A wrong headcount is worse than a missing one: it silently turns a
    20-person provider into a 1.7-million-employee one and passes every
    downstream size filter.
    """
    if value is None:
        return None
    try:
        count = int(value)
    except (TypeError, ValueError):
        return None
    if count < 0 or count > MAX_PLAUSIBLE_EMPLOYEE_COUNT:
        return None
    return count


def _has_ixbrl_profit_loss_exemption_marker(content: bytes) -> bool:
    text = content.decode("utf-8", errors="ignore")
    return any(pattern.search(text) for pattern in IXBRL_PROFIT_LOSS_EXEMPTION_PATTERNS)


def _row_has_segments(row: dict[str, Any]) -> bool:
    for key, value in row.items():
        if not key.startswith("segment:"):
            continue
        if str(value or "").strip():
            return True
    return False


def _pick_ixbrl_row(
    rows: list[dict[str, Any]],
    names: tuple[str, ...],
    *,
    instant: bool,
) -> dict[str, Any] | None:
    candidates = [row for row in rows if row.get("name") in names and row.get("value") is not None]
    if not candidates:
        return None

    def sort_key(row: dict[str, Any]) -> tuple[int, int, int, int]:
        try:
            name_rank = names.index(str(row.get("name")))
        except ValueError:
            name_rank = len(names)
        date_value = row.get("instant") if instant else (row.get("enddate") or row.get("instant"))
        ordinal = _parse_date(date_value).toordinal() if date_value else -1
        period_days = -1
        if row.get("startdate") and row.get("enddate"):
            start = _parse_date(row.get("startdate"))
            end = _parse_date(row.get("enddate"))
            if start is not None and end is not None:
                period_days = (end - start).days
        return (
            name_rank,
            1 if _row_has_segments(row) else 0,
            -ordinal,
            -period_days,
        )

    candidates.sort(key=sort_key)
    return candidates[0]


def _pick_ixbrl_value(
    rows: list[dict[str, Any]],
    names: tuple[str, ...],
    *,
    instant: bool,
) -> float | int | None:
    row = _pick_ixbrl_row(rows, names, instant=instant)
    if row is None:
        return None
    return row.get("value")


def _row_period_start(row: dict[str, Any]) -> date | None:
    return _parse_date(row.get("startdate"))


def _row_period_end(row: dict[str, Any]) -> date | None:
    return _parse_date(row.get("enddate") or row.get("instant"))


def _duration_row_sort_key(row: dict[str, Any]) -> tuple[int, int, int]:
    end_date = _row_period_end(row)
    start_date = _row_period_start(row)
    period_days = -1
    if start_date is not None and end_date is not None:
        period_days = (end_date - start_date).days
    return (
        1 if _row_has_segments(row) else 0,
        -(end_date.toordinal() if end_date is not None else -1),
        -period_days,
    )


def _pick_ixbrl_period_bounds(
    rows: list[dict[str, Any]],
) -> tuple[date | None, date | None]:
    revenue_row = _pick_ixbrl_row(rows, REVENUE_FACTS, instant=False)
    if revenue_row is not None:
        start = _row_period_start(revenue_row)
        end = _row_period_end(revenue_row)
        if start is not None or end is not None:
            return start, end

    net_assets_row = _pick_ixbrl_row(rows, NET_ASSETS_FACTS, instant=True)
    if net_assets_row is not None:
        balance_sheet_end = _row_period_end(net_assets_row)
        if balance_sheet_end is not None:
            for names in PERIOD_FACT_GROUPS:
                row = _pick_ixbrl_row(
                    [
                        candidate
                        for candidate in rows
                        if _row_period_end(candidate) == balance_sheet_end
                    ],
                    names,
                    instant=False,
                )
                if row is None:
                    continue
                start = _row_period_start(row)
                end = _row_period_end(row)
                if start is not None or end is not None:
                    return start, end

            matching_duration_rows = [
                row
                for row in rows
                if row.get("value") is not None
                and (row.get("startdate") or row.get("enddate"))
                and _row_period_end(row) == balance_sheet_end
            ]
            if matching_duration_rows:
                matching_duration_rows.sort(key=_duration_row_sort_key)
                return (
                    _row_period_start(matching_duration_rows[0]),
                    _row_period_end(matching_duration_rows[0]),
                )
            return None, balance_sheet_end

    for names in PERIOD_FACT_GROUPS:
        row = _pick_ixbrl_row(rows, names, instant=False)
        if row is None:
            continue
        start = _row_period_start(row)
        end = _row_period_end(row)
        if start is not None or end is not None:
            return start, end

    duration_rows = [
        row
        for row in rows
        if row.get("value") is not None and (row.get("startdate") or row.get("enddate"))
    ]
    if not duration_rows:
        return None, None

    duration_rows.sort(key=_duration_row_sort_key)
    start = _row_period_start(duration_rows[0])
    end = _row_period_end(duration_rows[0])
    return start, end


def _derived_total_assets(
    *,
    fixed_assets: float | None,
    current_assets: float | None,
) -> float | None:
    if fixed_assets is None and current_assets is None:
        return None
    return float((fixed_assets or 0.0) + (current_assets or 0.0))


def _parse_ixbrl_bytes(
    content: bytes,
) -> ParsedFinancialFacts:
    profit_loss_exempt = _has_ixbrl_profit_loss_exemption_marker(content)
    try:
        ixbrl = IXBRL(io.BytesIO(content), raise_on_error=False)
        rows = ixbrl.to_table(fields="numeric")
    except Exception as exc:
        return _empty_parsed_financials(
            parse_status="ixbrl_parse_error",
            parse_failure_reason=str(exc),
        )

    revenue = _pick_ixbrl_value(rows, REVENUE_FACTS, instant=False)
    employee_count_value = _pick_ixbrl_value(
        rows,
        EMPLOYEE_COUNT_FACTS,
        instant=False,
    )
    gross_profit = _pick_ixbrl_value(rows, GROSS_PROFIT_FACTS, instant=False)
    profit_before_tax = _pick_ixbrl_value(
        rows,
        PROFIT_BEFORE_TAX_FACTS,
        instant=False,
    )
    profit_after_tax = _pick_ixbrl_value(
        rows,
        PROFIT_AFTER_TAX_FACTS,
        instant=False,
    )
    fixed_assets = _pick_ixbrl_value(rows, FIXED_ASSETS_FACTS, instant=True)
    current_assets = _pick_ixbrl_value(
        rows,
        CURRENT_ASSETS_FACTS,
        instant=True,
    )
    net_assets = _pick_ixbrl_value(rows, NET_ASSETS_FACTS, instant=True)
    net_current_assets = _pick_ixbrl_value(
        rows,
        NET_CURRENT_ASSETS_FACTS,
        instant=True,
    )

    employee_count = coerce_employee_count(employee_count_value)
    filing_period_start, filing_period_end = _pick_ixbrl_period_bounds(rows)
    revenue_value = float(revenue) if revenue is not None else None
    gross_profit_value = (
        float(gross_profit) if gross_profit is not None else None
    )
    profit_before_tax_value = (
        float(profit_before_tax) if profit_before_tax is not None else None
    )
    profit_after_tax_value = (
        float(profit_after_tax) if profit_after_tax is not None else None
    )
    fixed_assets_value = (
        float(fixed_assets) if fixed_assets is not None else None
    )
    current_assets_value = (
        float(current_assets) if current_assets is not None else None
    )
    total_assets_value = _derived_total_assets(
        fixed_assets=fixed_assets_value,
        current_assets=current_assets_value,
    )
    net_assets_value = float(net_assets) if net_assets is not None else None
    net_current_assets_value = (
        float(net_current_assets) if net_current_assets is not None else None
    )
    status, reason = _classify_financial_facts(
        revenue=revenue_value,
        employee_count=employee_count,
        gross_profit=gross_profit_value,
        profit_before_tax=profit_before_tax_value,
        profit_after_tax=profit_after_tax_value,
        fixed_assets=fixed_assets_value,
        current_assets=current_assets_value,
        total_assets=total_assets_value,
        net_assets=net_assets_value,
        net_current_assets=net_current_assets_value,
        partial_status="partial",
        error_status="ixbrl_parse_error",
    )

    return ParsedFinancialFacts(
        revenue=revenue_value,
        employee_count=employee_count,
        filing_period_start=filing_period_start,
        filing_period_end=filing_period_end,
        gross_profit=gross_profit_value,
        profit_before_tax=profit_before_tax_value,
        profit_after_tax=profit_after_tax_value,
        fixed_assets=fixed_assets_value,
        current_assets=current_assets_value,
        total_assets=total_assets_value,
        net_assets=net_assets_value,
        net_current_assets=net_current_assets_value,
        parse_status=status,
        parse_failure_reason=reason,
        profit_loss_exempt=profit_loss_exempt,
    )


def _parse_numeric_text(value: str) -> float | None:
    cleaned = value.strip().replace(",", "").replace(" ", "")
    if not cleaned:
        return None
    negative = False
    if cleaned.startswith("(") and cleaned.endswith(")"):
        negative = True
        cleaned = cleaned[1:-1]
    try:
        parsed = float(cleaned)
    except ValueError:
        return None
    return -parsed if negative else parsed


def _regex_number(text: str, pattern: re.Pattern[str]) -> float | None:
    match = pattern.search(text)
    if not match:
        return None
    value = _parse_numeric_text(match.group("value"))
    return value


REVENUE_REGEXES = (
    re.compile(
        r"turnover(?:\s+and\s+other\s+income)?\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)",
        re.IGNORECASE,
    ),
    re.compile(
        r"revenue\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)",
        re.IGNORECASE,
    ),
)
EMPLOYEE_COUNT_REGEXES = (
    re.compile(
        r"average\s+number\s+of\s+employees(?:\s+during\s+the\s+period|\s+during\s+the\s+year)?\s*[:\-]?\s*(?P<value>\d{1,6})",
        re.IGNORECASE,
    ),
    re.compile(r"employees?\s*[:\-]?\s*(?P<value>\d{1,6})", re.IGNORECASE),
)
GROSS_PROFIT_REGEXES = (
    re.compile(
        r"gross\s+profit(?:\s+or\s+loss)?\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)",
        re.IGNORECASE,
    ),
)
PROFIT_BEFORE_TAX_REGEXES = (
    re.compile(
        r"profit\s+before\s+tax(?:ation)?\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)",
        re.IGNORECASE,
    ),
    re.compile(
        r"profit\s+on\s+ordinary\s+activities\s+before\s+tax(?:ation)?\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)",
        re.IGNORECASE,
    ),
)
PROFIT_AFTER_TAX_REGEXES = (
    re.compile(
        r"profit(?:\s+or\s+loss)?\s+after\s+tax(?:ation)?\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:profit|loss)\s+for\s+the\s+(?:financial\s+)?year\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)",
        re.IGNORECASE,
    ),
)
FIXED_ASSETS_REGEXES = (
    re.compile(
        r"fixed\s+assets\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)",
        re.IGNORECASE,
    ),
)
CURRENT_ASSETS_REGEXES = (
    re.compile(
        r"current\s+assets\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)",
        re.IGNORECASE,
    ),
)
NET_ASSETS_REGEXES = (
    re.compile(
        r"net\s+assets\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)",
        re.IGNORECASE,
    ),
    re.compile(
        r"shareholders'?[\s\-]+funds\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)",
        re.IGNORECASE,
    ),
)
NET_CURRENT_ASSETS_REGEXES = (
    re.compile(
        r"net\s+current\s+assets(?:\s*/\s*\(liabilities\))?\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)",
        re.IGNORECASE,
    ),
    re.compile(
        r"net\s+current\s+assets(?:\s+or\s+liabilities)?\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)",
        re.IGNORECASE,
    ),
)


def _first_regex_value(
    text: str,
    patterns: tuple[re.Pattern[str], ...],
) -> float | None:
    for pattern in patterns:
        value = _regex_number(text, pattern)
        if value is not None:
            return value
    return None


def _parse_pdf_bytes(
    content: bytes,
) -> ParsedFinancialFacts:
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            page_texts = [(page.extract_text() or "") for page in pdf.pages]
    except Exception as exc:
        return _empty_parsed_financials(
            parse_status="pdf_parse_error",
            parse_failure_reason=str(exc),
        )

    page_count = len(page_texts)
    text_len = sum(len(text.strip()) for text in page_texts)
    if page_count > 0 and text_len < page_count * 100:
        return _empty_parsed_financials(
            parse_status="pdf_no_text_layer",
            parse_failure_reason="pdf_no_text_layer",
        )

    text = "\n".join(page_texts)
    revenue = _first_regex_value(text, REVENUE_REGEXES)
    employee_count_value = _first_regex_value(text, EMPLOYEE_COUNT_REGEXES)
    employee_count = coerce_employee_count(employee_count_value)
    gross_profit = _first_regex_value(text, GROSS_PROFIT_REGEXES)
    profit_before_tax = _first_regex_value(
        text,
        PROFIT_BEFORE_TAX_REGEXES,
    )
    profit_after_tax = _first_regex_value(
        text,
        PROFIT_AFTER_TAX_REGEXES,
    )
    fixed_assets = _first_regex_value(text, FIXED_ASSETS_REGEXES)
    current_assets = _first_regex_value(text, CURRENT_ASSETS_REGEXES)
    total_assets = _derived_total_assets(
        fixed_assets=fixed_assets,
        current_assets=current_assets,
    )
    net_assets = _first_regex_value(text, NET_ASSETS_REGEXES)
    net_current_assets = _first_regex_value(text, NET_CURRENT_ASSETS_REGEXES)
    status, reason = _classify_financial_facts(
        revenue=revenue,
        employee_count=employee_count,
        gross_profit=gross_profit,
        profit_before_tax=profit_before_tax,
        profit_after_tax=profit_after_tax,
        fixed_assets=fixed_assets,
        current_assets=current_assets,
        total_assets=total_assets,
        net_assets=net_assets,
        net_current_assets=net_current_assets,
        partial_status="pdf_parse_partial",
        error_status="pdf_parse_error",
    )

    return ParsedFinancialFacts(
        revenue=revenue,
        employee_count=employee_count,
        filing_period_start=None,
        filing_period_end=None,
        gross_profit=gross_profit,
        profit_before_tax=profit_before_tax,
        profit_after_tax=profit_after_tax,
        fixed_assets=fixed_assets,
        current_assets=current_assets,
        total_assets=total_assets,
        net_assets=net_assets,
        net_current_assets=net_current_assets,
        parse_status=status,
        parse_failure_reason=reason,
    )
