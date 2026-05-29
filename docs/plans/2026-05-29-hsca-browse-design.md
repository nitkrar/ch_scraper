# HSCA Browse UI — Design

Date: 2026-05-29
Topic: Interactive HSCA browse sub-tab in the GUI's CQC pane
Status: Design (pre-implementation)

## Goal

Add an interactive browse/search/export surface for HSCA locations to the GUI.
Today the GUI only has HSCA ingest buttons (Sync / Download / Process) plus a
row count in the CQC pane. Users screening UK homecare M&A targets need to
filter and inspect HSCA locations the same way they already can for CQC
locations and providers.

## Decisions (from brainstorming)

1. **Placement** — third sub-view inside the existing `CQCPane`, alongside
   Locations and Providers. Not a new top-level pane (avoids duplicating the
   status/pagination/export scaffolding).
2. **Selector widget** — convert the existing `Locations / Providers`
   **Radiobuttons** into a `ttk.Notebook` tab selector with three tabs:
   Locations, Providers, HSCA.
3. **Data source** — HSCA browse joins three tables:
   - `cqc_hsca_locations h` (HSCA-specific columns)
   - **INNER JOIN** `cqc_locations l ON h.location_id = l.location_id`
     (real name / postcode / region / local_authority / service_types /
     is_active columns — these do NOT exist on the HSCA durable table; only
     `raw_row JSON` had them, and the CQC table provides them as indexed
     columns)
   - **LEFT JOIN** `companies c ON h.provider_companies_house_number =
     c.company_number` (company name + status for corporate-linkage screening)
4. **Join type** — INNER between HSCA and CQC locations: show only HSCA
   locations that also exist in the current CQC scrape. Guarantees every row
   has name/region/postcode populated and filters always behave. Trade-off:
   HSCA rows with no CQC match are not shown.
5. **Filters** — reuse the CQC Locations filter set (location name, provider
   name, postcode prefix, service type, region, local authority, active),
   plus one HSCA-only filter: **Has CH number** (Yes / No / All).

## Non-obvious facts that shaped this

- `cqc_hsca_locations.location_id` is the durable table's primary key, while
  `cqc_locations.location_id` is the match key backed by a unique index. The
  join is still clean, but the CQC side is not declared as the table PK in
  `bootstrap_locations.sql`.
- The HSCA durable table (`cqc_hsca_locations`) has NO name/postcode/region/LA
  columns; they live only in `raw_row JSON`. The CQC-locations join is what
  makes them filterable/indexed instead of JSON-extracted.
- The companies table is `SELECT * FROM companies_staging` plus tracking
  columns; join key is `company_number`, display fields `company_name` and
  `company_status`.
- `cqc_locations.service_types` is stored as a pipe-delimited `VARCHAR`, so the
  shared locations WHERE builder uses `_string_any_like(...)` there rather than
  the provider-side `_list_any(...)` helper.

## Architecture

Mirror the existing CQC layering exactly: query layer → API facade → GUI.

### Query layer — `ch_bulk/cqc/query.py`

- **Extend** `_build_cqc_locations_where(...)` with an optional `alias=""`
  parameter (rule #9, extend-don't-duplicate). Existing CQC callers pass
  nothing → bare column names, unchanged behaviour. HSCA passes `alias="l"` so
  joined columns disambiguate (`LOWER(l.name) LIKE …`, `l.region IN …`, etc.).
- `query_hsca_locations(db_path, *, name_contains, service_types, regions,
  local_authorities, provider_name_contains, postcode_prefix, is_active,
  has_ch_number=None, sort_by="name", sort_order="ASC", page=1, page_size=50)
  -> tuple[list[dict], int]`
  - Builds the shared WHERE via `_build_cqc_locations_where(..., alias="l")`.
  - Appends the HSCA-only condition for `has_ch_number`:
    `h.provider_companies_house_number IS NOT NULL` (True) /
    `IS NULL` (False) / omitted (None).
  - SELECT projects: `l.name`, `l.provider_name`, `c.company_name`,
    `c.company_status`, `h.provider_companies_house_number`,
    `h.number_of_beds`, `h.care_home`, `h.dormant`,
    `h.provider_ownership_type`, `h.provider_brand_name`, the `st_*` service
    booleans, `l.postcode`, `l.region`, `l.local_authority`,
    `l.service_types`, `l.is_active`, `h.location_id`.
  - FROM/JOIN per Decision 3. ORDER BY validated against an
    `HSCA_LOCATION_SORT_COLUMNS` whitelist (qualified column names), default
    `l.name`. LIMIT/OFFSET pagination identical to `query_cqc_locations`.
  - Returns `(rows, total)` where total = COUNT(*) over the same joined WHERE.
- **Reuse** `get_cqc_filter_options(db_path)` for the dropdowns — values come
  from `cqc_locations`, which is the join source. No new function.
- `export_hsca_locations_csv(db_path, output_path, **filters) -> int` —
  `COPY (<joined SELECT with WHERE>) TO '<path>' (HEADER, DELIMITER ',')`,
  same single-quote escaping as `export_cqc_locations_csv`. Returns row count.

### API facade — `ch_bulk/api.py`

- `query_hsca_locations_advanced(self, **filters) -> tuple[list[dict], int]`
  — thin pass-through to `query_hsca_locations`, same shape as
  `query_cqc_locations_advanced`.
- `export_hsca_locations_csv(self, output_path, **filters) -> int` — thin
  pass-through.
- Dropdown options reuse the existing `get_cqc_filter_options`.

### GUI — `ch_bulk/gui.py`, `CQCPane`

- **Tabs:** replace the `Locations / Providers` Radiobuttons with a
  `ttk.Notebook`. Three tabs (empty placeholder frames used purely as a
  segmented selector). The shared DB-status header stays above the notebook;
  the shared filter frame + paginated results tree stay below it. Bind
  `<<NotebookTabChanged>>` to the existing sub-view switch logic, but make the
  handler callable both from the notebook event and from the current
  `refresh()` path (`_on_subtab_change(event=None)` or equivalent). Either bind
  only after the dependent widgets exist or guard the handler during initial
  construction so an early tab-change event cannot crash the pane.
  - Rationale for shared filter/tree (not full content per tab): Locations and
    HSCA use the identical filter set; Providers is a near-subset. Duplicating
    the filter frame 3× would fight rule #9.
- `sub_view` gains a third value `"HSCA"`.
- `_build_results_tree` adds an HSCA column set: location name, provider name,
  company name, company status, CH number, beds, care_home, dormant,
  ownership type, brand, service-type flags, region, postcode, local
  authority.
- Filter frame for HSCA: same fields as Locations (provider name, location
  name, postcode, service type, region, LA, active) **plus** a "Has CH number"
  combobox (Yes / No / All). The Providers-only "Min active locations" field
  stays hidden for HSCA via the existing show/hide mechanism.
- `_get_filters`, `_run_query`, `_on_export`, and sort routing gain an HSCA
  branch when `sub_view == "HSCA"`; `_on_search` remains the thin page-reset
  wrapper that delegates to `_run_query`.

## Testing

Follow the existing stdlib `unittest` pattern (tests/, not pytest). New tests
should build a small fixture DuckDB directly in a dedicated module (there is no
existing `tests/test_cqc_query.py` helper/module to mirror):

- `query_hsca_locations` returns only rows present in both HSCA and CQC
  (INNER join correctness): an HSCA row with no matching CQC location is
  excluded.
- `has_ch_number=True/False/None` filters correctly.
- Company fields populate via the LEFT join; an HSCA row whose CH number has no
  companies match still appears with NULL company_name/status.
- Shared `_build_cqc_locations_where(alias="l")` produces qualified column
  names; `alias=""` (default) is byte-identical to current output
  (regression guard for the CQC path).
- `export_hsca_locations_csv` writes the expected row count and headers.
- API-level tests should instantiate `ChBulk` with both a temp `data_dir` and
  temp `db_path`, so `setup_logging(...)` stays inside the test sandbox instead
  of writing to the repo's default `data/logs`.

## Out of scope (YAGNI)

- No double-click drill-through from an HSCA row to the CH tab.
- No beds-range filter (beds is display-only; not selected as a filter).
- No new top-level pane.
- No interactive editing of HSCA rows.

## Files touched

- `ch_bulk/cqc/query.py` — `alias` param, `query_hsca_locations`,
  `export_hsca_locations_csv`, `HSCA_LOCATION_SORT_COLUMNS`.
- `ch_bulk/api.py` — `query_hsca_locations_advanced`,
  `export_hsca_locations_csv`.
- `ch_bulk/gui.py` — `CQCPane`: Notebook selector, HSCA sub-view, column set,
  filter wiring, search/export routing.
- `tests/` — new HSCA query/export tests.
