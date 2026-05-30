# Enrichment Capture + Backfill (Directors / CQC ratings / HSCA bands) — Design

Date: 2026-05-29
Topic: Promote already-scraped-but-unstored fields into durable columns, fix
go-forward capture, and backfill existing rows from on-disk raw. Data layer only.
Status: Design (pre-implementation)

## Goal

Three audit findings showed we scrape data we never persist into queryable
columns. Capture them going forward AND backfill existing rows from raw we
already hold — NO re-scraping. Surfacing on the GUI tabs is a deliberate
follow-up (data must exist first).

## Scope (3 areas) + storage decisions (user-confirmed)

### Area 1 — Directors (`company_enrichment`)
New columns:
- `total_active_directors INTEGER` — count of active director-role officers,
  independent of DOB presence.
- `directors JSON` — array of `{name, officer_role, appointed_on}` for active
  directors (per the "store in JSON" preference).

Source of truth: the `/officers` `items` array. Currently `get_officers`
(`ch_enricher.py:205`) returns `payload.get("items", [])` and
`compute_age_fields` (`ch_enricher.py:208-241`) consumes only DOB year + role +
resigned flag. The items carry `name`, `officer_role`, `appointed_on`,
`resigned_on`.

Backfill source: `cqc_api_responses` rows with `entity_type='ch_directors'`
(16,529 rows) — `raw_json` is the items array. Count + JSON-build are derivable
with NO re-scrape. (CH's own top-level `active_count` includes secretaries and
is not retained; counting director-role items ourselves is both more precise and
fully recoverable.) For backfill, select the latest response per
`company_number`/`entity_id` by `fetched_at` even though the current DB happens
to have one `ch_directors` row per company today.

Go-forward note: adding these columns is broader than extending
`COMPANY_ENRICHMENT_COLUMNS`. The live write/load surface in `ch_enricher.py`
also includes `_base_company_enrichment_row`, both
`_load_existing_company_enrichment*` SELECT lists, both
`_insert_or_replace_company_enrichment*` placeholder / JSON-cast paths, and
`_load_company_enrichment_from_batch`.

### Area 2 — CQC ratings (`cqc_providers_enriched` + `cqc_locations_enriched`)
New columns on BOTH tables:
- `rating_safe TEXT`, `rating_effective TEXT`, `rating_caring TEXT`,
  `rating_responsive TEXT`, `rating_well_led TEXT`.

Source: the existing `current_ratings JSON` column (already populated by
`*_ENRICH_INSERT_SQL` via `json_extract(s.raw_json,'$.currentRatings')`). The
5 "key question" ratings are nested:
- **Location**: `currentRatings.overall.keyQuestionRatings[]` — array of
  `{name, rating}` (names: Safe / Effective / Caring / Responsive / Well-led).
  Clean.
- **Provider**: use `currentRatings.overall.keyQuestionRatings[]` when
  `overall` exists; otherwise leave the 5 columns NULL. Live provider rows are
  mixed: some carry `overall`, some only `serviceRatings`, and some have both.
  Do NOT invent an aggregate / rollup from `serviceRatings[]`.

Backfill: `UPDATE` the 5 columns from the existing `current_ratings` JSON — no
re-scrape. Go-forward: extend `PROVIDER_ENRICH_INSERT_SQL` and
`LOCATION_ENRICH_INSERT_SQL`, which are the real durable write path used by the
staging loader. The `parse_*_payload()` helpers exist, but they are not what
persists rows during runtime loads.

`relationships` is ALREADY a stored JSON column on both tables — NO change /
NO backfill; surfacing it is later UI work, out of scope here.

### Area 3 — HSCA (`cqc_hsca_locations`)
New columns:
- `service_user_bands JSON` — array of active band names, storing the exact
  suffixes from the live ODS headers after stripping the prefix
  `"Service user band - "`:
  `Children 0-18 years`, `Dementia`,
  `Learning disabilities or autistic spectrum disorder`, `Mental Health`,
  `Older People`, `People detained under the Mental Health Act`,
  `People who misuse drugs and alcohol`, `People with an eating disorder`,
  `Physical Disability`, `Sensory Impairment`, `Whole Population`,
  `Younger Adults`.
- `regulated_activities JSON` — array of active regulated-activity names,
  storing the exact suffixes from the live ODS headers after stripping the
  prefix `"Regulated activity - "`:
  `Accommodation for persons who require nursing or personal care`,
  `Accommodation for persons who require treatment for substance misuse`,
  `Assessment or medical treatment for persons detained under the Mental Health Act 1983`,
  `Diagnostic and screening procedures`, `Family planning`,
  `Management of supply of blood and blood derived products`,
  `Maternity and midwifery services`, `Nursing care`, `Personal care`,
  `Services in slimming clinics`, `Surgical procedures`,
  `Termination of pregnancies`,
  `Transport services, triage and medical advice provided remotely`,
  `Treatment of disease, disorder or injury`.

Source: `raw_row JSON` already on every `cqc_hsca_locations` row. The ODS
columns are `"Service user band - <name>"` and `"Regulated activity - <name>"`,
value truthy ("Y") = active. Strip the prefix, collect truthy → array.

Backfill: `UPDATE` from `raw_row` — no re-ingest. Go-forward: extend the HSCA
staging row build in `processor.py` (`_build_hsca_location_rows()` plus
`HSCA_LOCATION_STAGE_COLUMNS`) so the arrays are computed from each record
before insert. `upsert_hsca_locations.sql` then passes the staged columns
through with `INSERT ... BY NAME`.

## Cross-cutting

### Schema migration (additive, backward-compatible)
- Add the new columns to the bootstrap SQL (so fresh DBs get them):
  `bootstrap_pipeline.sql` (company_enrichment, cqc_*_enriched) and
  `bootstrap_hsca_locations.sql` (cqc_hsca_locations).
- For EXISTING populated DBs, extend the existing schema-repair path in
  `db/bootstrap.py`: `ensure_pipeline_schema(con)` already uses
  `_column_exists()` / `_ensure_column()` guards over a live connection. The
  repo currently declares `duckdb>=1.0` rather than pinning a single exact
  version; the current environment does support `ALTER TABLE ... ADD COLUMN IF
  NOT EXISTS`, but the established project pattern is still the guarded
  connection-based repair path.
- Per project rule: migrations are optional; existing queries must not fail if
  not yet run. These columns are additive and nullable — no existing query
  breaks.

### Backfill entry point
One idempotent operation the user runs on the real 2.5 GB DB (like GUI smoke):
- Add a `migration backfill-enrichment` CLI command (under the existing
  `migration` subgroup, which currently only exposes `export` / `import`) +
  an API method `backfill_enrichment()`.
- It runs (in dependency-free order): directors backfill (parse
  `cqc_api_responses` ch_directors items), CQC ratings backfill (UPDATE from
  current_ratings), HSCA bands/activities backfill (UPDATE from raw_row).
- Idempotent: safe to re-run (UPDATEs overwrite; directors recompute). Reports
  per-area counts (rows updated).
- Directors backfill is the only one needing JSON parse of items in Python
  (count active director-role, build the JSON array, and choose the latest
  `fetched_at` row per company); ratings + HSCA are pure SQL UPDATEs over
  existing JSON columns.

## Non-obvious facts (from audit + code)
- `cqc_api_responses` durably holds 16,529 ch_directors + 37,864 provider +
  56,623 location raw payloads → all backfills are from the DB / on-disk, no API.
- Directors raw_json is the items array (top-level officer counts already gone),
  so total = count of director-role active items — recompute, don't read
  active_count.
- Provider `current_ratings` is mixed: some rows have `overall`, some only
  `serviceRatings`, and some both. Provider sub-ratings should therefore come
  from `overall.keyQuestionRatings` when present and otherwise remain NULL.
- HSCA `raw_row` is `json.dumps(record, sort_keys=True)` (`processor.py:333`) —
  keys are the exact ODS headers including the `"Service user band - "` /
  `"Regulated activity - "` prefixes.

## Testing (stdlib unittest, NOT pytest)
New `tests/test_enrichment_backfill.py` with in-test DuckDB fixtures:
- Directors: a `cqc_api_responses` ch_directors row whose items have 3 active
  directors (2 with DOB, 1 without) + 1 resigned + 1 secretary →
  `total_active_directors == 3`, `directors` JSON has the 3 active directors
  with name/role/appointed_on, secretary + resigned excluded.
- CQC location ratings: a `current_ratings` with overall.keyQuestionRatings →
  5 columns populated correctly; a row with no overall → 5 NULLs.
- CQC provider ratings: per the confirmed provider rule (overall-or-NULL).
- HSCA: a `raw_row` with two truthy "Service user band - X" and one truthy
  "Regulated activity - Y" → arrays contain exactly X and Y; falsy/blank flags
  excluded.
- Backfill idempotency: running twice yields identical column values.
- Migration: `ADD COLUMN IF NOT EXISTS` (or guarded equivalent) is safe to run
  on a DB that already has the columns.
- API / CLI tests that instantiate `ChBulk` should still pass a temp
  `data_dir`, even though the constructor accepts `None`, because logging is
  initialized under `data_dir`.

## Out of scope (YAGNI)
- No GUI surfacing (separate follow-up; data first).
- No re-scraping; no new API calls.
- `relationships` (already stored), provider rating aggregation, HSCA address/
  geo/other raw_row fields (not requested this round).

## Files touched
- `ch_bulk/db/sql/bootstrap_pipeline.sql` — new columns on company_enrichment +
  cqc_*_enriched.
- `ch_bulk/db/sql/cqc/bootstrap_hsca_locations.sql` — new HSCA columns.
- `ch_bulk/db/bootstrap.py` — extend `ensure_pipeline_schema(con)` /
  `_ensure_column()` repair for existing DBs.
- `ch_bulk/db/migration.py` — only if shared helper placement is useful; it is
  not currently a general migration registry.
- `ch_bulk/companies_house/ch_enricher.py` — go-forward directors capture.
- `ch_bulk/cqc/api_enricher.py` — go-forward 5-rating capture in
  `PROVIDER_ENRICH_INSERT_SQL` + `LOCATION_ENRICH_INSERT_SQL`.
- `ch_bulk/cqc/processor.py` (+ `db/sql/cqc/*hsca*`) — go-forward HSCA bands/
  activities capture in `_build_hsca_location_rows()` + stage columns.
- backfill: new code path + `migration backfill-enrichment` CLI + `api.py`
  method.
- `tests/test_enrichment_backfill.py` — new.
