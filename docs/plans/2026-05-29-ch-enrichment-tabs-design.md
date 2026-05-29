# CH Enrichment Tabs (Directors Age + Financials) — Design

Date: 2026-05-29
Topic: Add Directors Age + Financials browse sub-tabs to the GUI's CH pane
Status: Design (pre-implementation)

## Goal

Surface the `company_enrichment` data interactively. Today the CH pane is a
single companies browse; the per-company enrichment (director ages, financials)
has no UI. Add two browse sub-tabs — Directors Age and Financials — mirroring
the HSCA/CQC sub-tab pattern, so M&A target screening (succession risk, revenue
band, size) is filterable in the GUI.

## Decisions (from brainstorming)

1. **Placement** — convert `CHPane` to a `ttk.Notebook` with three tabs:
   **Companies** (existing), **Directors Age**, **Financials**. Same pattern as
   the CQC pane conversion (commit `e09bf84`).
2. **Browse model** — independent paginated browses (like CQC), not master/detail
   drill-down from a selected company.
3. **Data source** — both new tabs read `company_enrichment` (one row per
   `company_number`), joined for display/search:
   ```sql
   FROM company_enrichment e
     INNER JOIN companies c             ON e.company_number = c.company_number
     LEFT  JOIN current_company_match m ON e.company_number = m.company_number
     LEFT  JOIN cqc_providers p         ON m.cqc_provider_id = p.provider_id
   ```
4. **Mandatory data presence (INNER semantics within the enrichment row)** —
   each enrichment tab only shows companies that actually have that domain's
   data. `company_enrichment` holds BOTH director-age and financial fields in one
   row, so presence is enforced by a per-tab WHERE predicate, not a join:
   - Directors Age: `e.avg_director_age IS NOT NULL`
     (DOB-backed director age data present; companies with active directors but
     no DOB still end up with `avg_director_age = NULL` and should stay
     excluded from an age tab)
   - Financials:
     `COALESCE(e.revenue, e.employee_count, e.gross_profit,
     e.profit_before_tax, e.profit_after_tax, e.fixed_assets,
     e.current_assets, e.total_assets, e.net_assets, e.net_current_assets)
     IS NOT NULL`
   A company with directors-only data appears in Directors Age but not
   Financials, and vice versa. **The exact null semantics must be verified
   against the directors + financials enrichers** during review (so the
   predicate matches how nulls are actually written).
5. **Shared filters across all three tabs** — because every tab joins (or can
   join) `companies`, all company-level filters are common:
   - Search: company name / company number / provider name
   - SIC code(s), status, postcode prefix, incorporation year from/to, country,
     in-latest-scrape (active)
6. **Tab-specific filters**:
   - Directors Age: **Min director age** → `e.max_director_age >= ?` (returns
     any company with *any* director at/above the threshold, since the oldest
     director's age ≥ threshold ⟹ at least one qualifies).
   - Financials: **Min revenue** → `e.revenue >= ?`; **Min employees** →
     `e.employee_count >= ?`. (No "has financials" toggle — presence is
     mandatory per Decision 4.)
7. **Empty-state on CH-only DB (user decision)** — if the pipeline-schema
   objects the enrichment tabs depend on (`company_enrichment` and/or the
   `current_company_match` view) do not exist, the Directors Age + Financials
   tabs must render a graceful empty list with a message (e.g. "No enrichment
   data — run enrichment first."), NOT a raw DuckDB catalog error. Guard at the
   query layer (catch `duckdb.CatalogException` → return `([], 0)`, mirroring
   `get_cqc_filter_options`'s existing catalog-missing handling) and surface the
   message in the GUI count label.
8. **`company_type` filter: backend-only (user decision)** — the new query
   signatures keep `company_type` (consistent with `query_companies`) but NO GUI
   control is added for it. Available to API callers; not surfaced in the tabs.

## Coverage caveat

These tabs browse whatever rows currently exist in `company_enrichment`, which
is broader than just `current_company_match`:

- director enrichment targets SIC-scoped `companies` rows (or explicit company
  IDs), not just matched companies;
- financial-filings enrichment is driven by `current_company_match`;
- revenue estimation can also backfill `company_enrichment` rows from employee
  counts without requiring a current match.

So the two enrichment tabs are still "enriched-company browses", but unmatched
companies can legitimately appear there with `provider_name = NULL`.

## Non-obvious facts that shaped this

- Join key is `company_number` everywhere (`company_enrichment`, `companies`,
  `current_company_match`). Provider name lives in `cqc_providers.provider_name`
  (the bulk rollup table), linked via `current_company_match.cqc_provider_id`.
- `current_company_match` is a view created by `ensure_pipeline_schema(...)`,
  and its `ROW_NUMBER() ... WHERE rn = 1` shape guarantees at most one match row
  per `company_number`. Joined `COUNT(*)` queries therefore do not inflate.
- The real `current_company_match` macro depends on pipeline-schema objects
  including `cqc_hsca_locations`; tests should create a minimal fixture-local
  view rather than importing the full production SQL blindly.
- The existing companies WHERE builder is `_build_filter_where(...)` in
  `companies_house/query.py`; SIC handling delegates to `_build_sic_where(...)`
  over `sic_code_1..4` via exact `IN (...)` matching.
- The Companies tab currently has NO name/number search box; this design adds a
  shared search that also covers it.
- The current bare companies browse/export path uses `SELECT * FROM companies`.
  If a joined search path is added, it must switch to `SELECT c.*` (and
  qualified sort columns) to avoid duplicate `company_number`/ambiguous-column
  problems from the left joins.

## Architecture

Mirror the CQC/HSCA layering: query layer → API facade → GUI.

### Query layer — `ch_bulk/companies_house/query.py`

- **Extend** `_build_filter_where(...)` and `_build_sic_where(...)` with an
  optional `alias=""` parameter (rule #9). Default `""` keeps existing callers
  (`query_companies`, exports) byte-identical (unqualified columns). Enrichment
  queries pass `alias="c"` so company columns disambiguate in the joins.
- **Shared search helper** `_enrichment_search_where(search, *, alias_c="c",
  alias_p="p")` → returns
  `(LOWER({c}.company_name) LIKE ? OR {c}.company_number LIKE ?
    OR LOWER({p}.provider_name) LIKE ?)` + params, or `("", [])` when blank.
- **Shared join helper** `_enrichment_from_join()` → returns the FROM/JOIN block
  in Decision 3, reused by both queries and both exports.
- `query_directors_age(db_path, *, search=None, sic_codes=None, status=None,
  company_type=None, postcode_prefix=None, year_from=None, year_to=None,
  country=None, is_active=None, min_director_age=None, sort_by="company_name",
  sort_order="ASC", page=1, page_size=50) -> tuple[list[dict], int]`
  - WHERE = `e.avg_director_age IS NOT NULL`
    AND `_build_filter_where(..., alias="c")`
    AND `_enrichment_search_where(search)`
    AND (`e.max_director_age >= ?` when `min_director_age` set).
    Do **not** use `e.directors_dob_years IS NOT NULL` as the presence test:
    the directors enricher writes `[]` there for no-DOB/no-usable-age cases.
  - SELECT: `c.company_number`, `c.company_name`, `p.provider_name`,
    `e.avg_director_age`, `e.min_director_age`, `e.max_director_age`,
    `e.directors_over_60`, `e.all_directors_60_plus`.
  - Sort whitelist `DIRECTORS_SORT_COLUMNS` (qualified), default `c.company_name`.
  - COUNT + paged SELECT over the same WHERE; returns `(rows, total)`.
- `query_financials(db_path, *, search=None, <same company filters>,
  min_revenue=None, min_employees=None, sort_by="company_name",
  sort_order="ASC",
  page=1, page_size=50) -> tuple[list[dict], int]`
  - WHERE = `COALESCE(e.revenue, e.employee_count, e.gross_profit,
    e.profit_before_tax, e.profit_after_tax, e.fixed_assets,
    e.current_assets, e.total_assets, e.net_assets, e.net_current_assets)
    IS NOT NULL`
    AND `_build_filter_where(..., alias="c")`
    AND `_enrichment_search_where(search)`
    AND (`e.revenue >= ?` when set) AND (`e.employee_count >= ?` when set).
  - SELECT: `c.company_number`, `c.company_name`, `p.provider_name`,
    `e.revenue`, `e.revenue_source`, `e.employee_count`, `e.gross_profit`,
    `e.profit_before_tax`, `e.profit_after_tax`, `e.fixed_assets`,
    `e.current_assets`, `e.total_assets`, `e.net_assets`,
    `e.net_current_assets`, `e.filing_period_end`, `e.filing_age_months`.
  - Sort whitelist `FINANCIALS_SORT_COLUMNS` (qualified), default
    `c.company_name`. Keep `revenue` sortable, but do not assume valid
    financial rows always have non-NULL revenue (`partial_no_revenue` is real).
  - COUNT + paged SELECT; returns `(rows, total)`.
- `export_directors_age_csv` / `export_financials_csv(db_path, output_path,
  **filters) -> int` — `COPY (<same joined SELECT + WHERE>) TO ...`, same
  single-quote escaping as the existing CH exports. Return row count.
- **Companies-tab shared search** — add `search: str | None = None` to
  `query_companies` (and `export_filtered_csv`). When `search` is set, the query
  switches to a joined form
  (`FROM companies c LEFT JOIN current_company_match m LEFT JOIN cqc_providers p`)
  and applies `_enrichment_search_where(search)`, but the projection must become
  `SELECT c.*` and the validated sort columns must be qualified as `c.<col>`.
  **When `search` is blank, the query keeps the existing bare `FROM companies`
  fast path** — zero perf regression for the default 5.7M-row browse. The same
  conditional-join split applies to `export_filtered_csv`.

### API facade — `ch_bulk/api.py`

- `query_directors_age_advanced(self, **filters) -> tuple[list[dict], int]`
- `query_financials_advanced(self, **filters) -> tuple[list[dict], int]`
- `export_directors_age_csv(self, output_path, **filters) -> int`
- `export_financials_csv(self, output_path, **filters) -> int`
- `query_advanced` and `export_filtered_csv` already forward `**filters`, so the
  new `search` kwarg will flow automatically once
  `companies_house/query.py` accepts it. No dedicated facade wrapper change is
  required for `search`; match the existing module-scope import + thin
  pass-through pattern for the new enrichment methods only.

### GUI — `ch_bulk/gui.py`, `CHPane`

- **Notebook:** wrap the CH body in a `ttk.Notebook` with tabs Companies /
  Directors Age / Financials (empty page frames used as a selector; shared
  DB-status header + Download/Process/Sync stay above; shared filter frame +
  results tree + pagination stay below). Bind `<<NotebookTabChanged>>` to a
  guarded handler (`event=None`, no crash if fired during construction —
  same fix codex applied to CQC). Bind only after the tree/pagination widgets
  exist, or guard the handler accordingly. `refresh()` will also need to call
  the sub-view sync path after first build so the shown/hidden extra fields are
  correct.
- `sub_view` ∈ {`Companies`, `DirectorsAge`, `Financials`}.
- **Filter frame:** shared base fields always shown (search box [new], SIC,
  status, postcode, year from/to, country, active). Tab-specific extra fields
  shown/hidden per `sub_view` via the existing show/hide mechanism:
  - Directors Age → "Min director age" entry.
  - Financials → "Min revenue" + "Min employees" entries.
- **Columns:** add `DIRECTORS_COLUMNS` and `FINANCIALS_COLUMNS` constants
  alongside `CH_COLUMNS`; rebuild the tree's columns on tab change. The current
  CH pane builds its tree inline and `_display()` hardcodes `CH_COLUMNS`, so the
  refactor needs an active column-spec (`self._cols_spec`) or a tree-build
  helper, not just new constants.
- **Routing:** `_get_filters` collects the shared base + active tab's extras;
  `_run_query` / `_on_export` / `_on_sort` branch on `sub_view` to call
  `query_companies` (with search) / `query_directors_age_advanced` /
  `query_financials_advanced` and the matching exports. `_on_search` stays the
  thin page-reset wrapper.

## Testing

Stdlib `unittest` (NOT pytest). There is no existing CH enrichment browse test
module to mirror, so add a dedicated `tests/test_ch_enrichment_query.py`
building a small in-test DuckDB (companies, company_enrichment, `ch_cqc_matches`,
a minimal one-row-per-company `current_company_match` fixture view, and
`cqc_providers`). Cases:

- Directors tab excludes companies with `avg_director_age IS NULL`, including
  rows that represent active directors but no usable DOB-derived ages.
- Financials tab excludes rows whose financial fact columns are all NULL, but
  still includes `partial_no_revenue`-style rows where assets/employee counts
  exist and `revenue` remains NULL.
- `min_director_age` returns companies whose `max_director_age >= threshold`
  (any-director-above semantics).
- `min_revenue` / `min_employees` thresholds filter correctly.
- Search matches on company name, company number, AND provider name (via the
  match→provider join).
- Shared company filters (SIC, status, active) work through
  `_build_filter_where(alias="c")`.
- Empty-state: a DB without `company_enrichment` / `current_company_match`
  returns `([], 0)` from both enrichment queries (catalog-missing guard), not an
  exception.
- `_build_filter_where(alias="")` default output is byte-identical to current
  (regression guard for the existing companies query).
- `query_companies(search=...)` uses the joined form and matches provider name;
  `query_companies()` with no search keeps the bare `FROM companies` path, and
  the joined path projects `c.*` rather than `*`.
- Exports write expected row counts + headers.

## Out of scope (YAGNI)

- No profitability / filing-recency filters (not selected).
- No drill-down/master-detail from the Companies tab.
- No editing; no new top-level pane.
- No new enrichment producers (this is read-only over existing data).

## Files touched

- `ch_bulk/companies_house/query.py` — `alias` params, search/join helpers,
  `query_directors_age`, `query_financials`, two exports, `search` on
  `query_companies` + `export_filtered_csv`, two sort whitelists.
- `ch_bulk/api.py` — four new facade methods. `query_advanced` /
  `export_filtered_csv` already pass `**filters`, so `search` support lands via
  the query-layer signature change.
- `ch_bulk/gui.py` — `CHPane`: Notebook, shared search field, per-tab extras,
  column sets, routing.
- `tests/test_ch_enrichment_query.py` — new.
