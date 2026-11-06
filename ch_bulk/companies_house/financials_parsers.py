"""iXBRL and PDF parser helpers for financial filing enrichment."""

from __future__ import annotations

import io
import re
from collections import Counter
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
#     to negate, giving "-9 employees". The magnitude is the real headcount.
# The staff-costs case is not recoverable here, so bound the value instead.
# The PDF regex path already caps its match at six digits; hold every path to
# the same ceiling.
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
)
# Only locates the balance-sheet date for period detection; never a value.
BALANCE_SHEET_PERIOD_FACTS = NET_ASSETS_FACTS + ("TotalAssetsLessCurrentLiabilities",)
NET_CURRENT_ASSETS_FACTS = ("NetCurrentAssetsLiabilities",)
DEBTORS_FACTS = ("Debtors",)
CREDITORS_FACTS = ("Creditors",)
PROVISIONS_FOR_LIABILITIES_FACTS = (
    "ProvisionsForLiabilitiesBalanceSheetSubtotal",
    "TaxationIncludingDeferredTaxationBalanceSheetSubtotal",
    "Provisions",
)
DEFERRED_TAX_FACTS = (
    "DeferredTaxLiabilities",
    "NetDeferredTaxLiabilityAsset",
)
WITHIN_ONE_YEAR_MEMBERS = frozenset({"WithinOneYear", "CurrentFinancialInstruments"})
AFTER_ONE_YEAR_MEMBERS = frozenset({"AfterOneYear", "Non-currentFinancialInstruments"})
NO_MEMBERS: frozenset[str] = frozenset()
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
        debtors=None,
        creditors_within_one_year=None,
        creditors_after_one_year=None,
        provisions_for_liabilities=None,
        deferred_tax=None,
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
    balance_sheet_detail: dict[str, float | None] | None = None,
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
    # Only parsers that look for the detail fields report them missing.
    for name, value in (balance_sheet_detail or {}).items():
        if value is None:
            missing.append(f"{name}_missing")
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
    balance_sheet_detail: dict[str, float | None] | None = None,
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
            *(balance_sheet_detail or {}).values(),
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
                balance_sheet_detail=balance_sheet_detail,
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
    count = abs(count)
    if count > MAX_PLAUSIBLE_EMPLOYEE_COUNT:
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


def _row_dimension_members(row: dict[str, Any]) -> set[str]:
    """Local names of the row's dimension members, namespace prefix dropped.

    Segment values look like ``"explicitMember core:SomeDimension core:Member"``.
    """
    members: set[str] = set()
    for key, value in row.items():
        if not key.startswith("segment:"):
            continue
        parts = str(value or "").split(maxsplit=2)
        if parts:
            members.add(parts[-1].rsplit(":", 1)[-1])
    return members


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

    net_assets_row = _pick_ixbrl_row(rows, BALANCE_SHEET_PERIOD_FACTS, instant=True)
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


def _balance_sheet_date(rows: list[dict[str, Any]]) -> date | None:
    instants = [
        parsed
        for parsed in (_parse_date(row.get("instant")) for row in rows)
        if parsed is not None
    ]
    return max(instants) if instants else None


def _pick_balance_sheet_value(
    rows: list[dict[str, Any]],
    names: tuple[str, ...],
    *,
    balance_sheet_date: date | None,
    allowed_members: frozenset[str] | None = None,
    require_member: bool = False,
) -> float | int | None:
    """Pick an instant fact dated exactly at the balance-sheet date.

    A comparative (prior-year) fact is never returned. ``allowed_members``
    of None accepts any dimensions; otherwise every dimension member must be
    in the set, and ``require_member`` rejects facts with no dimensions.
    """
    if balance_sheet_date is None:
        return None
    eligible: list[dict[str, Any]] = []
    for row in rows:
        if _parse_date(row.get("instant")) != balance_sheet_date:
            continue
        if allowed_members is not None:
            members = _row_dimension_members(row)
            if require_member and not members:
                continue
            if not members <= allowed_members:
                continue
        eligible.append(row)
    return _pick_ixbrl_value(eligible, names, instant=True)


def _headcount_value(row: dict[str, Any] | None) -> float | int | None:
    """Use the displayed headcount when a negative scale made it fractional.

    ``scale="-2"`` on a displayed ``108`` parses as 1.08.
    """
    if row is None or row.get("value") is None:
        return None
    value = row["value"]
    if float(value).is_integer():
        return value
    displayed = _parse_numeric_text(str(row.get("displayed_text") or ""))
    if displayed is None or not displayed.is_integer():
        return value
    return -displayed if value < 0 else displayed


_TEXT_ROW_TAGS = ("tr", "p", "div", "li", "h1", "h2", "h3", "h4", "h5", "h6")
_STYLE_TOP_PATTERN = re.compile(r"(?:^|;)\s*top\s*:\s*([^;]+)")
_TEXT_AMOUNT = r"\d{1,3}(?:,\d{3})+"
TEXT_REVENUE_ROW_PATTERN = re.compile(
    rf"^(?:group\s+)?(?:turnover|revenue)\s+(?:\d{{1,2}}\s+)?£?\s*(?P<current>{_TEXT_AMOUNT})"
    rf"\s+£?\s*(?P<prior>{_TEXT_AMOUNT})(?![\d,.%])",
    re.IGNORECASE,
)


def _document_text_rows(soup: Any) -> list[str]:
    """Visible text split into rows, in document order.

    A row is a table row, or a leaf block element. Absolutely positioned
    sibling blocks sharing a ``top`` offset (one line of a PDF-style layout)
    are joined into a single row.
    """
    rows: list[str] = []
    last_key: tuple[int, str] | None = None
    for element in soup.find_all(_TEXT_ROW_TAGS):
        if element.name == "tr":
            if element.find("tr") is not None:
                continue
        elif element.find_parent("tr") is not None or element.find(_TEXT_ROW_TAGS):
            continue
        text = " ".join(element.get_text(" ").split())
        top_match = _STYLE_TOP_PATTERN.search(str(element.get("style") or ""))
        key = (
            (id(element.parent), top_match.group(1).strip())
            if top_match is not None
            else None
        )
        if key is not None and key == last_key:
            rows[-1] = f"{rows[-1]} {text}".strip()
        else:
            rows.append(text)
        last_key = key
    return [row for row in rows if row]


def _monetary_scale_factor(ixbrl: IXBRL) -> float:
    """Ratio of parsed to displayed value for the document's monetary facts."""
    scales = Counter(
        int(item.format.scale)
        for item in ixbrl.numeric
        if str(item.unit or "").lower().startswith("iso4217:")
        and item.format is not None
    )
    if not scales:
        return 1.0
    return float(10 ** scales.most_common(1)[0][0])


def _text_revenue(ixbrl: IXBRL) -> float | None:
    """Current-year turnover from a printed-but-untagged P&L row.

    The largest match wins: summary tables elsewhere in the document may
    print the same turnover rounded to thousands.
    """
    amounts = [
        amount
        for amount in (
            _parse_numeric_text(match.group("current"))
            for match in (
                TEXT_REVENUE_ROW_PATTERN.match(row)
                for row in _document_text_rows(ixbrl.soup)
            )
            if match is not None
        )
        if amount is not None
    ]
    if not amounts:
        return None
    return max(amounts) * _monetary_scale_factor(ixbrl)


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
        # to_table emits one row per numeric item, in order, but drops the
        # displayed text the headcount rule needs.
        for row, item in zip(rows, ixbrl.numeric):
            row["displayed_text"] = item.text
    except Exception as exc:
        return _empty_parsed_financials(
            parse_status="ixbrl_parse_error",
            parse_failure_reason=str(exc),
        )

    revenue = _pick_ixbrl_value(rows, REVENUE_FACTS, instant=False)
    if revenue is None:
        revenue = _text_revenue(ixbrl)

    employee_count_value = _headcount_value(
        _pick_ixbrl_row(rows, EMPLOYEE_COUNT_FACTS, instant=False)
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
    balance_sheet_date = _balance_sheet_date(rows)
    fixed_assets = _pick_balance_sheet_value(
        rows,
        FIXED_ASSETS_FACTS,
        balance_sheet_date=balance_sheet_date,
    )
    current_assets = _pick_balance_sheet_value(
        rows,
        CURRENT_ASSETS_FACTS,
        balance_sheet_date=balance_sheet_date,
    )
    net_assets = _pick_balance_sheet_value(
        rows,
        NET_ASSETS_FACTS,
        balance_sheet_date=balance_sheet_date,
    )
    net_current_assets = _pick_balance_sheet_value(
        rows,
        NET_CURRENT_ASSETS_FACTS,
        balance_sheet_date=balance_sheet_date,
    )
    debtors = _pick_balance_sheet_value(
        rows,
        DEBTORS_FACTS,
        balance_sheet_date=balance_sheet_date,
        allowed_members=WITHIN_ONE_YEAR_MEMBERS,
    )
    creditors_within_one_year = _pick_balance_sheet_value(
        rows,
        CREDITORS_FACTS,
        balance_sheet_date=balance_sheet_date,
        allowed_members=WITHIN_ONE_YEAR_MEMBERS,
        require_member=True,
    )
    creditors_after_one_year = _pick_balance_sheet_value(
        rows,
        CREDITORS_FACTS,
        balance_sheet_date=balance_sheet_date,
        allowed_members=AFTER_ONE_YEAR_MEMBERS,
        require_member=True,
    )
    provisions_for_liabilities = _pick_balance_sheet_value(
        rows,
        PROVISIONS_FOR_LIABILITIES_FACTS,
        balance_sheet_date=balance_sheet_date,
        allowed_members=NO_MEMBERS,
    )
    deferred_tax = _pick_balance_sheet_value(
        rows,
        DEFERRED_TAX_FACTS,
        balance_sheet_date=balance_sheet_date,
        allowed_members=NO_MEMBERS,
    )

    employee_count = coerce_employee_count(employee_count_value)
    filing_period_start, filing_period_end = _pick_ixbrl_period_bounds(rows)
    debtors_value = _optional_float(debtors)
    # Liabilities are stored as magnitudes: filers tag them with either sign.
    creditors_within_one_year_value = _optional_abs(creditors_within_one_year)
    creditors_after_one_year_value = _optional_abs(creditors_after_one_year)
    provisions_for_liabilities_value = _optional_abs(provisions_for_liabilities)
    # Signed: a negative deferred tax balance is an asset.
    deferred_tax_value = _optional_float(deferred_tax)
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
        balance_sheet_detail={
            "debtors": debtors_value,
            "creditors_within_one_year": creditors_within_one_year_value,
            "creditors_after_one_year": creditors_after_one_year_value,
            "provisions_for_liabilities": provisions_for_liabilities_value,
            "deferred_tax": deferred_tax_value,
        },
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
        debtors=debtors_value,
        creditors_within_one_year=creditors_within_one_year_value,
        creditors_after_one_year=creditors_after_one_year_value,
        provisions_for_liabilities=provisions_for_liabilities_value,
        deferred_tax=deferred_tax_value,
    )


def _optional_float(value: float | int | None) -> float | None:
    return float(value) if value is not None else None


def _optional_abs(value: float | int | None) -> float | None:
    return abs(float(value)) if value is not None else None


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
