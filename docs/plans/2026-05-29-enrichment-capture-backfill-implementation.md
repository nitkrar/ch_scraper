# Enrichment Capture + Backfill — Implementation Plan

**Goal**: Add durable columns for already-scraped fields (Directors total
count + JSON, CQC 5 sub-ratings, HSCA service-user bands + regulated activities
as JSON), fix go-forward capture, and backfill existing rows from on-disk raw.
Data layer only — no GUI, no re-scrape.

**Design**: `docs/plans/2026-05-29-enrichment-capture-backfill-design.md` (read first).

**Architecture**: Additive nullable columns (bootstrap + idempotent ALTER for
existing DBs). Backfill from durable raw: `cqc_api_responses` (directors items),
existing `current_ratings` JSON (CQC ratings), `raw_row` JSON (HSCA). Go-forward
capture wired into each enricher/processor. One idempotent
`migration backfill-enrichment` entry point.

**Tech stack**: Python 3, DuckDB, stdlib `unittest`.

## Hard Rules
1. **Code is source of truth.** Verify every JSON path / write path against live
   code before coding; fold divergences in.
2. **Tests = stdlib `unittest`, NOT pytest.** Full:
   `.venv/bin/python -m unittest discover -s tests` (baseline 126/OK). Single:
   `.venv/bin/python -m unittest tests.test_enrichment_backfill -v`.
3. **`core/paths.py` is the single source of truth for paths.**
4. **Additive + backward-compatible.** New columns nullable; migration optional;
   no existing query may fail if migration hasn't run. NO destructive schema ops.
5. **No re-scrape, no new API calls.** Backfill reads only durable raw / on-disk.
6. **VERTICAL TDD**: failing test first → red → minimal code → green, per step.
7. **Subcommits per step OK. DO NOT squash (main session does). DO NOT push.**
8. **Do NOT run the real-DB backfill yourself** — that's a user-run op (like GUI
   smoke). Implement + unit-test it on fixtures; STOP before running on
   `data/db/ch_bulk.duckdb`.
9. Fixed decisions: storage shapes per design (directors total INT + JSON; 5
   rating TEXT cols; HSCA bands/activities JSON); relationships untouched
   (already stored); no GUI work.

## Step 0: Recon (no code)
Confirm against live code and record findings in the test-module header:
- Directors items shape from `cqc_api_responses` ch_directors `raw_json`
  (`name`, `officer_role`, `appointed_on`, `resigned_on`, `date_of_birth`).
- The go-forward directors write path into `company_enrichment`
  (`ch_enricher.py` column list, `_base_company_enrichment_row`,
  `_load_existing_company_enrichment*`, `_insert_or_replace_company_enrichment*`,
  `_load_company_enrichment_from_batch`, `compute_age_fields`).
- CQC rating JSON paths: location `currentRatings.overall.keyQuestionRatings[]`;
  provider uses the same `overall.keyQuestionRatings[]` path when `overall`
  exists, else the 5 columns stay NULL. Do NOT aggregate from
  `serviceRatings[].keyQuestionRatings[]`.
- `*_ENRICH_INSERT_SQL` is the durable runtime write path
  (`json_extract(s.raw_json,...)` in `api_enricher.py`); `parse_*_payload()`
  helpers are test helpers unless deliberately kept in sync too.
- Repo dependency is `duckdb>=1.0`, not a single pinned version. Current env
  supports `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, but the codebase already
  uses guarded `_ensure_column(con, ...)` repair inside `ensure_pipeline_schema(con)`.
- HSCA: `raw_row` key format + exact live ODS header names; go-forward capture
  belongs in `_build_hsca_location_rows()` + `HSCA_LOCATION_STAGE_COLUMNS`.

---

## Step 1: Schema — new columns (bootstrap + idempotent migration)

**Files**: `db/sql/bootstrap_pipeline.sql`, `db/sql/cqc/bootstrap_hsca_locations.sql`,
`db/bootstrap.py`, `tests/test_enrichment_backfill.py`.

### 1a. Failing test
```python
def test_migration_adds_columns_idempotent(self):
    # build a DB at the OLD schema (no new cols), run ensure/migrate, assert cols exist
    ensure_enrichment_columns(self.con)
    cols = column_names(self.db, "company_enrichment")
    self.assertIn("total_active_directors", cols)
    self.assertIn("directors", cols)
    ensure_enrichment_columns(self.con)  # second run must not raise
```

### 1b. Run red → 1c. Implement
- Add to bootstrap SQL: `company_enrichment` → `total_active_directors INTEGER`,
  `directors JSON`; `cqc_providers_enriched` + `cqc_locations_enriched` →
  `rating_safe/effective/caring/responsive/well_led TEXT`;
  `cqc_hsca_locations` → `service_user_bands JSON`, `regulated_activities JSON`.
- Extend `ensure_pipeline_schema(con)` with the existing guarded
  `_ensure_column()` pattern (or a small connection-based helper it calls)
  rather than introducing a db-path-only migration hook.

### 1d. green → 1e. subcommit
`git commit -m "enrich-backfill: additive columns + idempotent migration"`

---

## Step 2: Directors backfill + go-forward

**Files**: `ch_bulk/companies_house/ch_enricher.py`, test module.

### 2a. Failing test
Fixture `cqc_api_responses` ch_directors row, items = [2 active directors w/ DOB,
1 active director no DOB, 1 resigned director, 1 active secretary].
```python
def test_directors_backfill_counts_and_json(self):
    backfill_directors(self.db)
    row = fetch(self.db, "SELECT total_active_directors, directors FROM company_enrichment WHERE company_number='X'")
    self.assertEqual(row.total_active_directors, 3)
    names = {d["name"] for d in json.loads(row.directors)}
    self.assertEqual(names, {"DIR ONE","DIR TWO","DIR THREE"})  # secretary + resigned excluded
def test_compute_directors_meta_pure(self):
    meta = compute_director_meta(items, )  # name/role/appointed_on + count
    self.assertEqual(meta["total_active_directors"], 3)
```

### 2b. red → 2c. Implement
- Add `compute_director_meta(officers) -> {total_active_directors, directors}`
  next to `compute_age_fields` (active = role contains "director" and not
  `resigned_on`; capture name/officer_role/appointed_on).
- Go-forward: include these in the full company_enrichment write/load path
  (`COMPANY_ENRICHMENT_COLUMNS`, `_base_company_enrichment_row`, both
  `_load_existing_company_enrichment*` SELECT lists, both
  `_insert_or_replace_company_enrichment*` placeholder / JSON-cast paths, and
  `_load_company_enrichment_from_batch`) so future enrichment emits them and
  existing row merges stay consistent.
- Backfill: `backfill_directors(db_path)` reads the latest
  `cqc_api_responses` `entity_type='ch_directors'` row per company_number /
  `entity_id` by `fetched_at`, computes meta from the stored items array, and
  UPDATEs `company_enrichment`. Reuse `compute_director_meta`.

### 2d. green → 2e. subcommit
`git commit -m "enrich-backfill: directors total count + JSON (capture + backfill)"`

---

## Step 3: CQC ratings backfill + go-forward

**Files**: `ch_bulk/cqc/api_enricher.py`, test module.

### 3a. Failing test
```python
def test_location_ratings_backfill(self):
    backfill_cqc_ratings(self.db)
    r = fetch(self.db, "SELECT rating_safe, rating_well_led FROM cqc_locations_enriched WHERE location_id='L1'")
    self.assertEqual(r.rating_safe, "Good"); self.assertEqual(r.rating_well_led, "Good")
def test_location_no_overall_yields_nulls(self):
    backfill_cqc_ratings(self.db)
    r = fetch(self.db, "... WHERE location_id='L_no_overall'")
    self.assertIsNone(r.rating_safe)
def test_provider_rating_rule(self):
    ...  # per the confirmed overall-or-NULL rule
```

### 3b. red → 3c. Implement
- Define the JSON extraction for the 5 key-question ratings (location:
  `currentRatings.overall.keyQuestionRatings`; provider uses the same path when
  `overall` exists, else NULL).
- Go-forward: extend `LOCATION_ENRICH_INSERT_SQL` / `PROVIDER_ENRICH_INSERT_SQL`
  to populate the 5 columns from `raw_json` (DuckDB JSON list filtering /
  `json_each` over `$.currentRatings.overall.keyQuestionRatings` to pick the
  element where name = 'Safe', etc.). Do not route runtime persistence through
  the Python `parse_*_payload()` helpers.
- Backfill: `backfill_cqc_ratings(db_path)` UPDATEs the 5 columns on both tables
  directly from each row's existing `current_ratings` column (no join back to
  `cqc_api_responses`, no re-scrape).

### 3d. green → 3e. subcommit
`git commit -m "enrich-backfill: CQC 5 sub-ratings (capture + backfill)"`

---

## Step 4: HSCA bands + regulated activities backfill + go-forward

**Files**: `ch_bulk/cqc/processor.py`, `db/sql/cqc/*hsca*`, test module.

### 4a. Failing test
```python
def test_hsca_bands_activities_backfill(self):
    backfill_hsca_flags(self.db)
    r = fetch(self.db, "SELECT service_user_bands, regulated_activities FROM cqc_hsca_locations WHERE location_id='H1'")
    self.assertEqual(set(json.loads(r.service_user_bands)), {"Dementia","Older People"})
    self.assertEqual(set(json.loads(r.regulated_activities)), {"Personal care"})
```
(Fixture `raw_row` has `"Service user band - Dementia":"Y"`,
`"Service user band - Older People":"Y"`, `"Service user band - Mental Health":""`,
`"Regulated activity - Personal care":"Y"`, others blank.)

### 4b. red → 4c. Implement
- Helper `extract_hsca_flags(raw_row_dict) -> {service_user_bands, regulated_activities}`
  collecting keys with the given prefix whose value is truthy, prefix stripped.
  Use the exact live ODS header names currently present in `raw_row`
  (`Service user band - Children 0-18 years`, ...,
  `Regulated activity - Treatment of disease, disorder or injury`).
- Go-forward: compute both arrays in `_build_hsca_location_rows()` and carry
  them through `HSCA_LOCATION_STAGE_COLUMNS`; the upsert SQL then persists the
  staged columns by name so re-ingest populates them.
- Backfill: `backfill_hsca_flags(db_path)` UPDATEs the two columns from each
  row's `raw_row`.

### 4d. green → 4e. subcommit
`git commit -m "enrich-backfill: HSCA service-user bands + regulated activities (capture + backfill)"`

---

## Step 5: Unified backfill entry point (CLI + API)

**Files**: `ch_bulk/api.py`, `ch_bulk/cli.py`, test module.

### 5a. Failing test
```python
def test_backfill_enrichment_runs_all_and_reports(self):
    from ch_bulk import ChBulk
    ch = ChBulk(db_path=self.db, data_dir=self.tmp)
    summary = ch.backfill_enrichment()
    self.assertIn("directors_updated", summary)
    self.assertIn("ratings_updated", summary)
    self.assertIn("hsca_updated", summary)
def test_backfill_idempotent(self):
    ch.backfill_enrichment(); a = snapshot(self.db)
    ch.backfill_enrichment(); b = snapshot(self.db)
    self.assertEqual(a, b)
```

### 5b. red → 5c. Implement
- `ChBulk.backfill_enrichment()` → calls `ensure_enrichment_columns` then the
  three backfills, returns a summary dict of per-area counts.
- CLI: `ch-bulk migration backfill-enrichment` under the existing `migration`
  subgroup (currently `export` / `import` only), printing the summary.
- Tests that instantiate `ChBulk` should keep passing a temp `data_dir` even
  though the constructor accepts `None`, because logging is initialized under
  `data_dir`.

### 5d. green → 5e. subcommit
`git commit -m "enrich-backfill: migration backfill-enrichment CLI + API"`

---

## Step 6: Full suite + STOP (user runs real-DB backfill)

### 6a. Full suite green
```bash
.venv/bin/python -m unittest discover -s tests
```
Expect baseline 126 + new backfill tests, all green.

### 6b. STOP — post STATUS. Do NOT run on the real DB, do NOT squash.
The user runs the one-time backfill on `data/db/ch_bulk.duckdb`:
```bash
.venv/bin/ch-bulk migration backfill-enrichment
```
and spot-checks populated columns. Main session squashes to a single
`Phase 9: enrichment capture + backfill` commit AFTER user confirms (no push).

## Files touched
- `ch_bulk/db/sql/bootstrap_pipeline.sql`, `db/sql/cqc/bootstrap_hsca_locations.sql`
- `ch_bulk/db/bootstrap.py`
- `ch_bulk/companies_house/ch_enricher.py`
- `ch_bulk/cqc/api_enricher.py`
- `ch_bulk/cqc/processor.py` (+ hsca SQL)
- `ch_bulk/api.py`, `ch_bulk/cli.py`
- `tests/test_enrichment_backfill.py` (new)
