# CH Enrichment Tabs (Directors Age + Financials) — Implementation Plan

**Goal**: Add Directors Age + Financials browse sub-tabs to the GUI's CH pane,
backed by joined queries over `company_enrichment`, and convert `CHPane` to a
`ttk.Notebook` (Companies / Directors Age / Financials).

**Design**: `docs/plans/2026-05-29-ch-enrichment-tabs-design.md` (read first).

**Architecture**: Mirror the CQC/HSCA query → API facade → GUI layering. Reuse
`_build_filter_where` (extended with `alias`). Enrichment tabs join
`company_enrichment e INNER JOIN companies c LEFT JOIN current_company_match m
LEFT JOIN cqc_providers p`. Each enrichment tab enforces a data-presence WHERE
predicate. Shared search (name/number/provider) across all three tabs; Companies
tab adds the match/provider join only when a search term is present.

**Tech stack**: Python 3, DuckDB, Tkinter/ttk. Tests = stdlib `unittest`.

## Hard Rules

1. **Code is source of truth, not this doc.** If a signature/column/null
   semantic diverges from live code, trust the code and fold it in.
2. **Tests = stdlib `unittest`, NOT pytest.** Full:
   `.venv/bin/python -m unittest discover -s tests` (baseline 108/OK — keep
   green). Single: `.venv/bin/python -m unittest tests.test_ch_enrichment_query -v`.
3. **`core/paths.py` is the single source of truth for paths.**
4. **Backward compat:** `alias` defaults to `""` → existing companies callers
   byte-identical. `search` defaults to `None` → Companies query keeps the bare
   `FROM companies` fast path. No migration; no schema change.
5. **Verify presence predicates against the enrichers.** Before implementing the
   Directors/Financials WHERE, read the directors + financials enrichers to
   confirm how nulls are written (does a company with directors-but-no-DOB get
   `avg_director_age = NULL`?). Adjust the predicate to match reality.
6. **VERTICAL TDD**: per step, failing test first → run red → minimal code → run
   green. No bulk test-then-code.
7. **Subcommits per step OK; DO NOT squash (main session does), DO NOT push.**
8. **Do not change fixed product decisions**: 3 CH tabs, independent browses,
   mandatory data presence per tab, shared company filters + search, tab extras
   (min director age; min revenue + min employees), no "has financials" toggle.

## Task dependency table

| Group | Steps | Parallelize | Depends on |
|-------|-------|-------------|------------|
| 0 | Step 0 (enricher null-semantics recon) | No | — |
| 1 | Step 1 (alias on `_build_filter_where`/`_build_sic_where`) | No | 0 |
| 2 | Steps 2–4 (helpers, `query_directors_age`, `query_financials`) | No (same file) | 1 |
| 3 | Step 5 (`search` on `query_companies`) | No | 1 |
| 4 | Step 6 (exports) | No | 2 |
| 5 | Step 7 (API facade) | No | 2–4 |
| 6 | Step 8 (CHPane Notebook), Step 9 (HSCA-style wiring) | No (same file) | 5 |
| 7 | Step 10 (full suite + GUI smoke) | No | all |

---

## Step 0: Recon — enricher null semantics (no code)

Read the directors + financials enrichers (e.g. `companies_house/ch_enricher.py`
and `companies_house/financials_*.py`) and confirm:
- When does `avg_director_age` end up NULL vs 0? In the live directors enricher,
  companies with no usable DOB-derived ages keep `avg/min/max_director_age =
  NULL`, but `directors_over_60 = 0`, `all_directors_60_plus = FALSE`, and
  `directors_dob_years = []`.
- Which financial columns define real "has financials" presence? In the live
  staging/upsert path, partial rows can have `employee_count`, `fixed_assets`,
  `current_assets`, `total_assets`, `net_assets`, or `net_current_assets`
  populated while revenue/profit fields stay NULL. Metadata fields such as
  `revenue_source` / `filing_period_end` / `filing_age_months` are **not**
  reliable presence predicates on their own (`pdf_no_text_layer` rows can carry
  metadata while all financial facts remain NULL).
Record findings as a comment in the test module header. Adjust Step 2/3 WHERE
predicates if reality differs from the design's proposed predicates.

---

## Step 1: `alias` param on companies WHERE builders (regression-safe)

**File**: `ch_bulk/companies_house/query.py`, `tests/test_ch_enrichment_query.py`.

### 1a. Failing test
```python
import unittest
from ch_bulk.companies_house.query import _build_filter_where

class TestFilterWhereAlias(unittest.TestCase):
    def test_default_unqualified(self):
        where, _ = _build_filter_where(status="active")
        self.assertIn("company_status = ?", where)
        self.assertNotIn("c.company_status", where)
    def test_alias_qualifies(self):
        where, _ = _build_filter_where(status="active", postcode_prefix="SW1",
                                       is_active=True, alias="c")
        self.assertIn("c.company_status = ?", where)
        self.assertIn("STARTS_WITH(c.postcode", where)
        self.assertIn("c.is_active = ?", where)
```

### 1b. Run red
```bash
.venv/bin/python -m unittest tests.test_ch_enrichment_query.TestFilterWhereAlias -v
```

### 1c. Implement
Add `alias: str = ""` to `_build_filter_where` and `_build_sic_where`. Helper
`q = lambda col: f"{alias}.{col}" if alias else col`; wrap `company_status`,
`company_type`, `postcode`, `incorporation_date`, `country_of_origin`,
`is_active`, and the SIC columns (`sic_code_1..4`, threaded into
`_build_sic_where`). `_build_sic_where` does exact `IN (...)` matching across
the four SIC columns, so the qualified path should preserve that shape exactly.
Params unchanged.

### 1d. Run green → 1e. Subcommit
```bash
git add -A && git commit -m "ch-enrich: alias param on companies where-builders"
```

---

## Step 2: Helpers + `query_directors_age`

**File**: `ch_bulk/companies_house/query.py`, test module.

### 2a. Failing test
Build an in-test DuckDB with `companies`, `company_enrichment`, `ch_cqc_matches`,
a **minimal fixture-local** `current_company_match` view, and `cqc_providers`.
Do not import the production macros view verbatim in tests; the real view also
references `cqc_hsca_locations`. Rows:
- A1: directors age data (avg=65, max=70), matched to provider "Acme Group", revenue present.
- A2: directors age data (avg=40, max=45), no match, no revenue.
- A3: active directors but no usable DOB-derived age data (`avg/min/max = NULL`,
  `directors_over_60 = 0`, `all_directors_60_plus = FALSE`,
  `directors_dob_years = '[]'`), revenue present.
```python
def test_directors_excludes_null_age(self):
    rows, total = query_directors_age(self.db)
    self.assertEqual({r["company_number"] for r in rows}, {"A1", "A2"})
def test_min_director_age_any_director(self):
    rows, _ = query_directors_age(self.db, min_director_age=60)
    self.assertEqual({r["company_number"] for r in rows}, {"A1"})
def test_search_matches_provider_name(self):
    rows, _ = query_directors_age(self.db, search="acme group")
    self.assertEqual({r["company_number"] for r in rows}, {"A1"})
```

### 2b. Run red
```bash
.venv/bin/python -m unittest tests.test_ch_enrichment_query.TestDirectors -v
```

### 2c. Implement
Add `_enrichment_from_join()`, `_enrichment_search_where(search)`,
`DIRECTORS_SORT_COLUMNS`. Implement `query_directors_age(...)` per design:
presence `e.avg_director_age IS NOT NULL` + `_build_filter_where(alias="c")` +
search + `e.max_director_age >= ?`. Use `e.avg_director_age IS NOT NULL` as the
presence predicate, not `directors_dob_years IS NOT NULL` (the live enricher
uses `[]` there for no-DOB rows). Explicit SELECT projection, COUNT + paged
SELECT, `(rows, total)`.

**Empty-state guard (user decision — applies to BOTH enrichment queries):** wrap
the connect/execute in `try/except duckdb.CatalogException: return ([], 0)` so a
CH-only DB lacking `company_enrichment` / `current_company_match` returns empty
instead of raising (mirror the existing pattern in
`cqc/query.py:get_cqc_filter_options`). Add a test that drops/omits
`company_enrichment` and asserts `query_directors_age(self.db) == ([], 0)`.

**`company_type` (user decision):** keep `company_type` in the signature
(threaded into `_build_filter_where`), but no GUI control is added for it later.

### 2d. Run green → 2e. Subcommit
```bash
git add -A && git commit -m "ch-enrich: query_directors_age + join/search helpers"
```

---

## Step 3: `query_financials`

**File**: `ch_bulk/companies_house/query.py`, test module.

### 3a. Failing test
```python
def test_financials_excludes_no_data(self):
    rows, _ = query_financials(self.db)
    self.assertNotIn("A2", {r["company_number"] for r in rows})  # no financials
def test_min_revenue(self):
    rows, _ = query_financials(self.db, min_revenue=1_000_000)
    self.assertTrue(all(r["revenue"] >= 1_000_000 for r in rows))
def test_min_employees(self):
    rows, _ = query_financials(self.db, min_employees=10)
    self.assertTrue(all((r["employee_count"] or 0) >= 10 for r in rows))
```

### 3b. Run red
```bash
.venv/bin/python -m unittest tests.test_ch_enrichment_query.TestFinancials -v
```

### 3c. Implement
`query_financials(...)`: presence `COALESCE(e.revenue, e.employee_count,
e.gross_profit, e.profit_before_tax, e.profit_after_tax, e.fixed_assets,
e.current_assets, e.total_assets, e.net_assets, e.net_current_assets) IS NOT
NULL` + `_build_filter_where(alias="c")` + search + `e.revenue >= ?` +
`e.employee_count >= ?`. Do not use metadata columns such as
`revenue_source` / `filing_period_end` / `filing_age_months` as the presence
test. SELECT per design, `FINANCIALS_SORT_COLUMNS`, COUNT + paged SELECT.
Default `sort_by` should be `company_name`, not `revenue`, because valid
financial rows can still have NULL revenue (`partial_no_revenue`).

### 3d. Run green → 3e. Subcommit
```bash
git add -A && git commit -m "ch-enrich: query_financials"
```

---

## Step 4: (folded into 2–3) sort whitelists verified

Confirm both sort whitelists map GUI-clickable headings → qualified columns;
invalid `sort_by` falls back to default. Add a quick test asserting a bad
`sort_by` does not raise and returns default-ordered rows.
Subcommit if separate: `git commit -m "ch-enrich: sort whitelists"`.

---

## Step 5: `search` on `query_companies` (conditional join)

**File**: `ch_bulk/companies_house/query.py`, test module.

### 5a. Failing test
```python
def test_companies_search_blank_uses_bare_path(self):
    rows, total = query_companies(self.db)          # existing behavior
    self.assertGreaterEqual(total, 1)
def test_companies_search_provider(self):
    rows, _ = query_companies(self.db, search="acme group")
    self.assertEqual({r["company_number"] for r in rows}, {"A1"})
```

### 5b. Run red
```bash
.venv/bin/python -m unittest tests.test_ch_enrichment_query.TestCompaniesSearch -v
```

### 5c. Implement
Add `search: str | None = None` to `query_companies`. When `search` truthy:
`FROM companies c LEFT JOIN current_company_match m ON ... LEFT JOIN
cqc_providers p ON ...`, prepend `_enrichment_search_where(search)` to the WHERE,
and qualify the existing filter columns by passing `alias="c"` to
`_build_filter_where`. In the joined path, switch the projection to `SELECT c.*`
and qualify the validated sort column as `c.<column>` so duplicate
`company_number` columns from the joins do not corrupt the result mapping. Count
is still safe because `current_company_match` is one row per company. When
`search` falsy: keep the exact current bare `FROM companies` query (no alias, no
joins) — assert byte-identical SQL path.

### 5d. Run green → 5e. Subcommit
```bash
git add -A && git commit -m "ch-enrich: shared search on query_companies (conditional join)"
```

---

## Step 6: Exports

**File**: `ch_bulk/companies_house/query.py`, test module.

### 6a. Failing test
```python
def test_export_directors(self):
    out = self.tmp / "dir.csv"
    n = export_directors_age_csv(self.db, out, min_director_age=60)
    self.assertEqual(n, 1)
    self.assertIn("provider_name", out.read_text().splitlines()[0])
```

### 6b. Run red → 6c. Implement
`export_directors_age_csv` / `export_financials_csv(db_path, output_path,
**filters) -> int`: reuse `_enrichment_from_join` + the same WHERE assembly as
the queries (extract a private `_directors_where(**f)` / `_financials_where(**f)`
returning `(where_sql, params)` shared by query + export), `COPY (...) TO`,
return COUNT.

### 6d. Run green → 6e. Subcommit
```bash
git add -A && git commit -m "ch-enrich: directors + financials CSV export"
```

---

## Step 7: API facade

**File**: `ch_bulk/api.py`, test module.

### 7a. Failing test
```python
def test_api_directors(self):
    from ch_bulk import ChBulk
    ch = ChBulk(db_path=self.db, data_dir=self.tmp)  # pass temp data_dir to contain setup_logging output
    rows, total = ch.query_directors_age_advanced(min_director_age=60)
    self.assertEqual(total, 1)
```

### 7b. Run red → 7c. Implement
Add `query_directors_age_advanced`, `query_financials_advanced`,
`export_directors_age_csv`, `export_financials_csv` to `ChBulk` (module-scope
imports, thin pass-through on `self.db_path`, matching existing CQC/HSCA facade
methods). `query_advanced` and `export_filtered_csv` already forward `**filters`,
so once the underlying query-layer functions accept `search`, that kwarg will
pass through automatically; no dedicated facade code change is required there.

### 7d. Run green → 7e. Subcommit
```bash
git add -A && git commit -m "ch-enrich: api facade methods + search passthrough"
```

---

## Step 8: Convert CHPane → ttk.Notebook (3 tabs)

**File**: `ch_bulk/gui.py` (`CHPane`).

> GUI has no unit tests; verify by import + suite + manual smoke.

### 8a. Implement
Wrap the CH body in `ttk.Notebook` (tabs Companies / Directors Age /
Financials; empty page frames as selector). Keep DB-status header +
Download/Process/Sync above; shared filter frame + tree + pagination below.
Bind `<<NotebookTabChanged>>` to a guarded `_on_subtab_change(event=None)` (no
crash during construction). Introduce `self.sub_view` (default `"Companies"`).
Unlike the current CQC pane, CH currently builds its tree inline and
`_display()` hardcodes `CH_COLUMNS`, so extract/store an active column spec
(`self._cols_spec`) as part of this refactor. Bind the notebook only after the
tree/pagination widgets exist, or guard the handler accordingly.

### 8b. Verify
```bash
.venv/bin/python -c "import ch_bulk.gui"
.venv/bin/python -m unittest discover -s tests
```

### 8c. Subcommit
```bash
git add -A && git commit -m "ch-enrich: convert CHPane to ttk.Notebook"
```

---

## Step 9: Wire Directors Age + Financials sub-views

**File**: `ch_bulk/gui.py` (`CHPane`).

### 9a. Implement
- Add `DIRECTORS_COLUMNS`, `FINANCIALS_COLUMNS` constants; rebuild tree columns
  per `sub_view`.
- Filter frame: add shared **Search** entry (always shown). Add per-tab extras
  shown/hidden by `sub_view`: Directors → "Min director age"; Financials →
  "Min revenue" + "Min employees".
- `_get_filters`: collect shared base (search + SIC/status/postcode/year/country/
  active) + active tab extras → map to the query kwargs.
- `_run_query` / `_on_export` / `_on_sort`: branch on `sub_view` →
  `query_companies(search=...)` / `query_directors_age_advanced` /
  `query_financials_advanced` and matching exports. Pagination + count identical
  to the Companies path. Add `self.sort_by_dir` / `self.sort_by_fin` defaults.
- `_display`: switch from hardcoded `CH_COLUMNS` to the active
  `self._cols_spec` so rebuilt trees and inserted row values stay aligned.
- `_on_clear`: reset the shared search field, per-tab extra filters, and the
  per-tab sort defaults alongside the existing company filters.
- Empty-state (user decision): when an enrichment query returns `([], 0)` on the
  Directors/Financials sub-views, set the count label to a clear message
  ("No enrichment data — run enrichment first.") rather than "0 results", so a
  CH-only DB reads as missing-data, not empty-filter. (No GUI control for
  `company_type` — it stays backend-only.)

### 9b. Verify
```bash
.venv/bin/python -c "import ch_bulk.gui"
.venv/bin/python -m unittest discover -s tests
```

### 9c. Subcommit
```bash
git add -A && git commit -m "ch-enrich: wire Directors Age + Financials sub-views"
```

---

## Step 10: End-to-end verification + GUI smoke

### 10a. Full suite (baseline + new tests green)
```bash
.venv/bin/python -m unittest discover -s tests
```

### 10b. GUI smoke (user-run — STOP here, do not run as executor)
```bash
.venv/bin/ch-bulk ui
```
Checklist:
- CH pane shows three real tabs (Companies / Directors Age / Financials).
- Search box filters by company name, number, AND provider name on all tabs.
- Companies tab with empty search still browses fast (no join).
- Directors Age: only companies with director ages; Min director age filters.
- Financials: only companies with financials; Min revenue + Min employees filter.
- SIC/status/etc shared filters work on all tabs; pagination + sort + export OK.

### 10c. Squash (main session, after user OK; DO NOT push)
```bash
git reset --soft <first subcommit^> && \
  git commit -m "Phase 8: CH Directors Age + Financials browse tabs"
```

## Files touched

- `ch_bulk/companies_house/query.py`
- `ch_bulk/api.py`
- `ch_bulk/gui.py`
- `tests/test_ch_enrichment_query.py` (new)
