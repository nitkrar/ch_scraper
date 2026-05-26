"""
keep_page(text) -> (keep, score, signals)

Heuristic page filter for OCR'd UK Companies House annual accounts.
Designed against 10 PDFs / 238 pages.

Strategy:
  1. Hard KEEP if a strong financial-statement anchor (balance sheet, income
     statement, SOFA, cash flow, statement of changes in equity, or notes
     section heading) co-occurs with line-item keywords or numeric content.
  2. Hard KEEP if the page is a numbers-dense note (commas + line items
     like trade debtors / creditors / fixed assets / employee counts).
  3. Hard DROP if the page is dominated by prose anchors (auditor opinion,
     trustees report, accounting-policies-only, social-value marketing).
  4. Score-based fallback for everything else.
"""

import re

# --- patterns ---
COMMA_NUM = re.compile(r"\d{1,3}(?:,\d{3})+")
YEAR_PAIR = re.compile(r"\b20\d{2}\s+20\d{2}\b")
POUND_THOUS = re.compile(r"£\s*['’]?\s*0{3}", re.I)

# Section anchors (financial-statement primary pages)
PRIMARY_STATEMENTS = re.compile(
    r"\b(BALANCE\s+SHEET|INCOME\s+STATEMENT|PROFIT\s+AND\s+LOSS|"
    r"STATEMENT\s+OF\s+COMPREHENSIVE\s+INCOME|"
    r"STATEMENT\s+OF\s+FINANCIAL\s+POSITION|"
    r"STATEMENT\s+OF\s+FINANCIAL\s+ACTIVIT|"
    r"STATEMENT\s+OF\s+CHANGES\s+IN\s+EQUITY|"
    r"STATEMENT\s+OF\s+CASH\s*FLOWS?|CASH\s*FLOW\s+STATEMENT)\b",
    re.I,
)

# Line-item / note keywords (presence boosts confidence)
LINE_ITEMS = re.compile(
    r"\b("
    r"turnover|revenue|gross\s+profit|cost\s+of\s+sales|"
    r"operating\s+profit|profit\s+before\s+tax|profit\s+after\s+tax|"
    r"net\s+assets|total\s+assets|net\s+current\s+assets|fixed\s+assets|"
    r"current\s+assets|tangible\s+(?:fixed\s+)?assets|intangible\s+assets|"
    r"trade\s+debtors|trade\s+receivables|trade\s+creditors|"
    r"debtors|creditors|"
    r"cash\s+at\s+bank|cash\s+and\s+cash\s+equivalents|"
    r"called\s+up\s+share\s+capital|retained\s+earnings|"
    r"deferred\s+tax|corporation\s+tax|taxation|"
    r"directors[’'`]?\s+remuneration|emoluments|"
    r"average\s+(?:monthly\s+)?number\s+of\s+employees|"
    r"average\s+number\s+of\s+(?:persons|staff|employees)|"
    r"wages\s+and\s+salaries|staff\s+costs|"
    r"donations\s+and\s+legacies|income\s+from\s+(?:charitable|investments)|"
    r"restricted\s+funds|unrestricted\s+funds|designated\s+funds|"
    r"reconciliation|net\s+cash"
    r")\b",
    re.I,
)

# Prose anchors that strongly suggest DROP
AUDIT_OPINION = re.compile(
    r"\b(independent\s+auditor[s’'`]*?\s+report|"
    r"we\s+have\s+audited|in\s+our\s+opinion|"
    r"basis\s+for\s+(?:our\s+)?opinion|"
    r"qualified\s+opinion|unqualified\s+opinion|"
    r"auditor[s’'`]*?\s+responsibilit|"
    r"responsibilities\s+of\s+(?:the\s+)?(?:trustees|directors)|"
    r"matters\s+on\s+which\s+we\s+are\s+required\s+to\s+report)\b",
    re.I,
)

STRATEGIC_PROSE = re.compile(
    r"\b(strategic\s+report|directors[’'`]?\s+report|"
    r"trustees[’'`]?\s+report|report\s+of\s+the\s+trustees|"
    r"section\s+172\s+statement|going\s+concern\s+statement|"
    r"principal\s+activit|future\s+developments|"
    r"public\s+benefit|reference\s+and\s+administrative)\b",
    re.I,
)

POLICY_ONLY = re.compile(
    r"\b(accounting\s+polic|basis\s+of\s+preparation|"
    r"basis\s+of\s+accounting|critical\s+accounting\s+estimates|"
    r"new\s+(?:and\s+revised\s+)?standards)\b",
    re.I,
)

NOTES_HEADER = re.compile(r"\bNOTES?\s+TO\s+THE\s+(FINANCIAL\s+STATEMENTS|ACCOUNTS)\b", re.I)

TOC_HINT = re.compile(r"^\s*CONTENTS\s*$", re.I | re.M)
COVER_HINT = re.compile(
    r"\b(company\s+registration\s+number|registered\s+number|"
    r"pages\s+for\s+filing|annual\s+report\s+and\s+(?:unaudited\s+)?financial)\b",
    re.I,
)


def keep_page(text: str) -> tuple[bool, int, dict]:
    if not text or not text.strip():
        return False, 0, {"reason": "empty"}

    t = text
    tlen = len(t)

    commas = len(COMMA_NUM.findall(t))
    digits = sum(c.isdigit() for c in t)
    digit_density = digits / max(tlen, 1)
    year_pair = bool(YEAR_PAIR.search(t))
    pound_thous = bool(POUND_THOUS.search(t))

    primary = bool(PRIMARY_STATEMENTS.search(t))
    notes_header = bool(NOTES_HEADER.search(t))
    line_item_hits = len(LINE_ITEMS.findall(t))

    audit = bool(AUDIT_OPINION.search(t))
    strategic = bool(STRATEGIC_PROSE.search(t))
    policy_only = bool(POLICY_ONLY.search(t))

    toc = bool(TOC_HINT.search(t)) and tlen < 600
    cover = bool(COVER_HINT.search(t)) and tlen < 500 and commas == 0

    sig = {
        "len": tlen, "commas": commas, "digits": digits,
        "digit_density": round(digit_density, 3),
        "year_pair": year_pair, "pound_thous": pound_thous,
        "primary": primary, "notes_header": notes_header,
        "line_items": line_item_hits,
        "audit": audit, "strategic": strategic, "policy_only": policy_only,
        "toc": toc, "cover": cover,
    }

    # --- hard DROP cascade ---
    if toc or cover:
        return False, -100, {**sig, "reason": "toc_or_cover"}

    # Auditor's opinion pages: long prose with audit phrases and almost no numbers
    if audit and commas <= 1 and digit_density < 0.04 and not primary:
        return False, -90, {**sig, "reason": "audit_opinion"}

    # Strategic / trustees / directors report prose with no real numbers
    if strategic and commas == 0 and digit_density < 0.03 and not primary and line_item_hits < 2:
        return False, -80, {**sig, "reason": "strategic_prose"}

    # Policy-only pages: policy keyword, no commas, no line items, no statement
    if policy_only and commas == 0 and line_item_hits < 2 and not primary and digit_density < 0.025:
        return False, -70, {**sig, "reason": "policy_only_prose"}

    # --- hard KEEP cascade ---
    # Primary statement page: must have commas (real number table) OR
    # employee-count line items with digits (single-employee disclosure pages).
    if primary and commas >= 2:
        return True, 100, {**sig, "reason": "primary_statement"}
    if primary and line_item_hits >= 2 and digit_density > 0.04:
        return True, 95, {**sig, "reason": "primary_statement_numeric"}

    # Notes page with strong number signal
    if notes_header and commas >= 4:
        return True, 90, {**sig, "reason": "notes_numeric"}

    # Notes page with line items + commas (catches small tables) OR
    # employee count notes (line items + digit density but no commas).
    if notes_header and line_item_hits >= 2 and commas >= 2:
        return True, 80, {**sig, "reason": "notes_lineitems"}
    if notes_header and re.search(r"average\s+(?:monthly\s+)?number\s+of\s+employees", t, re.I):
        return True, 75, {**sig, "reason": "notes_employee_count"}

    # Strong comma density anywhere (financial table)
    if commas >= 4:
        return True, 70, {**sig, "reason": "comma_dense"}

    # Year-pair header + some numbers + a line item = table page
    if year_pair and line_item_hits >= 1 and digits >= 40:
        return True, 60, {**sig, "reason": "year_pair_table"}

    # £'000 header + line items + numbers
    if pound_thous and line_item_hits >= 1 and digits >= 30:
        return True, 55, {**sig, "reason": "pound_thous_table"}

    # Prose with embedded numbers (restated figures, financial review with £X,XXX)
    if commas >= 2 and line_item_hits >= 2:
        return True, 50, {**sig, "reason": "prose_with_numbers"}

    # --- score fallback ---
    # If we're past the hard-cascades and this page has no commas AND no
    # year-pair AND no £'000 anchor, it's almost certainly prose. Drop fast.
    if commas == 0 and not year_pair and not pound_thous:
        return False, -50, {**sig, "reason": "no_numeric_anchors"}

    score = 0
    score += min(commas, 8) * 5
    score += min(line_item_hits, 6) * 4
    score += int(digit_density * 100)
    if primary: score += 30
    if notes_header: score += 20
    if year_pair: score += 10
    if pound_thous: score += 10
    if audit: score -= 30
    if strategic: score -= 25
    if policy_only: score -= 15

    keep = score >= 30
    sig["reason"] = "score_fallback"
    return keep, score, sig
