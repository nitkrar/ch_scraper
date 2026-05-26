# Homecare Pipeline Extension — Design + Implementation Plan

**Date:** 2026-05-24
**Status:** Draft for adversarial review
**Scope:** Extend `ch_scraper` (ch_bulk) to absorb the work currently in the sibling `uk-homecare-deals` toolkit. Move from Excel-centric per-run scripts to DB-centric pipelines, with a GUI surface that mirrors today's CH/CQC panes.

---

## Why

The user is an investment banker sourcing UK homecare M&A targets. Today she runs two siblings:

- `ch_scraper` — bulk CH + CQC ingest into DuckDB, GUI for browsing
- `uk-homecare-deals` — eleven skills + Python scripts that read her Excel workbook, enrich it, classify provider websites, score/tier targets, export deliverable

The Excel workbook is the source of truth in the second project. That has known problems:

1. Director-age enrichment, classification, and tiering rerun against Excel every time — no audit trail of model verdicts or matches
2. The CH refresh script hits the `/advanced-search` 10K cap (returns ~4K of an expected 11K). `ch_scraper`'s bulk download approach is the known fix and isn't wired in
3. The CH↔CQC reconciliation, Excluded list cross-check, classification history, and website storage all live in transient Excel cells
4. Classification today is keyword regex (the `batch_runner.py` "LLM fallback" never actually calls an LLM)

This plan ports the operational logic into `ch_scraper`, leveraging its existing bulk pipeline, schema, and GUI.

## Scope

In scope:
- New DuckDB tables for excluded lists, CH↔CQC matches, classifications, websites, CQC API enrichment
- CQC API scraper (provider + location endpoints) writing raw + parsed forms
- HSCA "Care directory with filters" bulk file ingest (the 122-col monthly ODS)
- GUI panes for the new workflows
- `tiered_targets` view + macros
- xlsx + csv export

Out of scope:
- Bulk download/sync mechanism (already exists in CHPane / CQCPane)
- CIW (Welsh regulator) — no API/feed
- Homecare Association UK directory — paywalled
- Multi-user / auth — single user, single machine
- Real-time progress streaming UI — synchronous Tk with progress bar per pane

## Decisions locked through design conversation

| # | Topic | Decision |
|---|---|---|
| 1 | Excluded list | Two tables — `exclusion_lists` (with `is_active` soft delete) + `excluded_companies` (keyed on `company_number`). One-time staging table resolves the legacy workbook import where the company_number was lost. New excludes always carry the number. |
| 2 | CH↔CQC match | Multi-signal scoring (100 for CH number, 60 exact-name+full-postcode, 50 prev_name match, fuzzy tiers 35/20/10 by postcode strictness, 25 fuzzy prev_name, 15 aggressive-normalize bonus). Auto-confirm ≥80, review 50–79, ignore <50. `match_signals` JSON for auditability. Default = score new pairs; full-rerun toggle preserves `user_confirmed`/`user_rejected`. |
| 3 | Classifications | Separate row per (company_number, source_type) — website / cqc_service_type / cqc_specialism / manual / rule. UPSERT on re-classify. Priority view picks current verdict (manual > website > cqc_service_type > cqc_specialism > rule). `sources_agreement` flag on view exposes disagreement. |
| 4 | Websites | One row per (company_number, discovered_via) — cqc_provider / cqc_location / web_search / manual. UPSERT on re-discover. `last_seen_reachable_at` updated by classification fetch (no separate checks pipeline). Auto-seeded from HSCA Provider/Location Web Address. |
| 5 | Per-row evidence | Folded into classifications row: `evidence_quote`, `source_url`, `classifier`. |
| 6 | Re-run triggers | GUI panes mirror existing pattern. Pipeline button = enrich + match + classify chain (NOT bulk — bulk has its own sync buttons). Sanity gate verifies bulk data exists before pipeline. Per-step buttons everywhere. **Halt-on-error semantics**: each step has explicit upstream dependencies; on failure the pipeline halts and the user sees exactly which downstream steps are now invalid. "Continue Anyway" only enabled when the failed step has no hard-required outputs for any downstream step in scope. Default = stop. |
| 7 | Tier engine | SQL view over (companies × company_enrichment × classification view × matches × excluded). Scoring via DuckDB SQL macros. Recomputes on read. |
| 8 | Export | xlsx (multi-tab: Summary, Tier 1, Tier 2, Tier 3, Excluded, Needs Review, Disagreements) + csv (single flat dump of view). |

### Explicit non-decisions / drops (anti-overengineering)

- No history tables on `classifications`, `company_websites`, `ch_cqc_matches` — UPSERT, lose prior values. Audit only via `classification_batches.model_version` (kept) and `cqc_api_responses` raw JSON (kept).
- No `web_search_candidates` table — re-run `web_search.py "<name>" --raw` when she asks "why X?"
- No `etag` on `cqc_api_responses` — CQC doesn't expose useful ones
- No prompt_version on `classification_batches` — we don't version prompts
- No `reviewed_by` columns — single user
- No `superseded_by` FK chains
- No `notes` free-text on operational tables
- No cron / scheduler — buttons only

---

## Schema

### Existing tables (touched, not replaced)

- **`companies`** (65 cols) — CH bulk, unchanged
- **`cqc_locations`** (20 cols) — CQC 15-col directory bulk, unchanged
- **`cqc_providers`** (12 cols) — rollup, unchanged

### New tables — bulk-derived

**`cqc_hsca_locations`** — hybrid model. Flat columns for fields downstream actually consumes; the remaining ~100 cols of the ODS land in a `raw_row JSON` column for later access without schema churn.

Flat columns:
- `location_id` (PK), `provider_id`
- `provider_companies_house_number`, `provider_charity_number`, `provider_ownership_type`
- `provider_brand_id`, `provider_brand_name`
- `provider_web_address`, `location_web_address`
- `care_home` (BOOLEAN), `number_of_beds` (INTEGER), `dormant` (BOOLEAN)
- `registered_manager_name`
- Service-type flags consumed by classifier: `st_domiciliary_care_service`, `st_supported_living_service`, `st_care_home_with_nursing`, `st_care_home_without_nursing`, `st_extra_care_housing_services`, `st_hospice_services_at_home` (BOOLEANs)
- `bulk_imported_at`, `bulk_file_date`

JSON column:
- `raw_row JSON` — the full row from the ODS, including the other 27 service type flags, 14 regulated activity flags, 12 service user bands, structured address line 2/lat/lon/uprn/paf, etc.

When a new downstream consumer needs a field that's in `raw_row` but not flat, promote it to a flat column. Avoids designing for hypothetical use.

**`cqc_hsca_dual_registrations`** — sister sheet from the same file. Multi-provider locations. Single-tab table, no JSON splitting (small, simple).

### New tables — exclusion

```sql
exclusion_lists (
  list_name        TEXT PRIMARY KEY,
  source           TEXT,                    -- 'workbook-import' / 'manual' / 'csv-upload'
  is_active        BOOLEAN NOT NULL DEFAULT TRUE,
  created_at       TIMESTAMP,
  last_imported_at TIMESTAMP
);

excluded_companies (
  company_number TEXT NOT NULL,
  list_name      TEXT REFERENCES exclusion_lists(list_name),
  reason         TEXT,
  excluded_at    TIMESTAMP,
  PRIMARY KEY (company_number, list_name)
);

-- one-time, truncate-and-reload on each import
excluded_staging (
  row_id          INTEGER PRIMARY KEY,
  raw_name        TEXT,
  raw_postcode    TEXT,
  raw_reason      TEXT,
  resolved_number TEXT,
  match_method    TEXT,                     -- 'exact' / 'fuzzy' / 'unmatched'
  match_score     INTEGER,
  needs_review    BOOLEAN
);
```

### New tables — match

```sql
ch_cqc_matches (
  company_number   TEXT,
  cqc_provider_id  TEXT,
  total_score      INTEGER NOT NULL,
  match_signals    JSON,                    -- ['ch_number'] or ['fuzzy_name_outward_pc']
  status           TEXT NOT NULL,           -- 'auto_confirmed' / 'needs_review' / 'user_confirmed' / 'user_rejected'
  matched_at       TIMESTAMP,
  PRIMARY KEY (company_number, cqc_provider_id)
);

-- One-row-per-company view of the current authoritative match.
-- Downstream (tier view, website seed) must read THIS, not ch_cqc_matches directly,
-- to avoid duplication when a company has multiple candidate pairs.
CREATE VIEW current_company_match AS
SELECT company_number, cqc_provider_id, total_score, match_signals, status, matched_at
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY company_number
      ORDER BY CASE status
        WHEN 'user_confirmed' THEN 1
        WHEN 'auto_confirmed' THEN 2
        WHEN 'needs_review'   THEN 3
      END, total_score DESC
    ) AS rn
  FROM ch_cqc_matches
  WHERE status IN ('user_confirmed', 'auto_confirmed', 'needs_review')
) WHERE rn = 1;
```

### New tables — CH enrichment (director ages, revenue)

```sql
company_enrichment (
  company_number          TEXT PRIMARY KEY,
  avg_director_age        INTEGER,
  min_director_age        INTEGER,
  max_director_age        INTEGER,
  directors_over_60       INTEGER,
  all_directors_60_plus   BOOLEAN,
  directors_dob_years     JSON,             -- [1958, 1962, 1967] for "why is avg 62" auditability
  revenue                 DOUBLE,
  revenue_source          TEXT,             -- 'filed_accounts' / 'employee_band_lookup'
  employee_count          INTEGER,
  last_enriched_at        TIMESTAMP
);
```

### New tables — CQC API enrichment

```sql
cqc_api_responses (
  response_id      INTEGER PRIMARY KEY,
  batch_id         UUID REFERENCES cqc_sync_batches(batch_id),
  entity_type      TEXT NOT NULL,           -- 'provider' / 'location'
  entity_id        TEXT NOT NULL,
  fetched_at       TIMESTAMP NOT NULL,
  scrape_date      DATE NOT NULL,           -- indexed for cleanup
  http_status      INTEGER,
  raw_json         JSON NOT NULL,
  UNIQUE (entity_type, entity_id, fetched_at)
);

cqc_providers_enriched (
  provider_id              TEXT PRIMARY KEY,
  companies_house_number   TEXT,
  charity_number           TEXT,
  ownership_type           TEXT,
  brand_id                 TEXT,
  brand_name               TEXT,
  company_name             TEXT,            -- legal entity name (≠ trading)
  registration_date        DATE,
  deregistration_date      DATE,
  registration_status      TEXT,
  postal_address_line_1    TEXT,
  postal_address_line_2    TEXT,
  postal_town              TEXT,
  postal_county            TEXT,
  postcode                 TEXT,
  region                   TEXT,
  local_authority          TEXT,
  latitude                 DOUBLE,
  longitude                DOUBLE,
  main_phone_number        TEXT,
  website                  TEXT,
  nominated_individual     TEXT,
  main_partner             TEXT,
  inspection_directorate   TEXT,
  current_overall_rating   TEXT,
  current_ratings          JSON,            -- subratings per key question
  regulated_activities     JSON,
  relationships            JSON,            -- linked-org history
  number_of_locations      INTEGER,
  last_inspection_date     DATE,
  last_report_date         DATE,
  api_response_id          INTEGER REFERENCES cqc_api_responses(response_id),
  enriched_at              TIMESTAMP
);

cqc_locations_enriched (
  location_id              TEXT PRIMARY KEY,
  provider_id              TEXT,
  care_home                BOOLEAN,
  number_of_beds           INTEGER,
  dormancy                 BOOLEAN,
  registration_date        DATE,
  deregistration_date      DATE,
  registration_status      TEXT,
  postal_address_line_1    TEXT,
  postal_address_line_2    TEXT,
  postal_town              TEXT,
  postal_county            TEXT,
  postcode                 TEXT,
  region                   TEXT,
  local_authority          TEXT,
  latitude                 DOUBLE,
  longitude                DOUBLE,
  uprn                     TEXT,
  paf                      TEXT,
  main_phone_number        TEXT,
  website                  TEXT,
  registered_manager_name  TEXT,
  registered_manager_absent_date DATE,
  inspection_directorate   TEXT,
  primary_inspection_category TEXT,
  current_overall_rating   TEXT,
  current_ratings          JSON,
  gac_service_types        JSON,
  specialisms              JSON,
  regulated_activities     JSON,
  relationships            JSON,
  last_inspection_date     DATE,
  last_report_date         DATE,
  api_response_id          INTEGER REFERENCES cqc_api_responses(response_id),
  enriched_at              TIMESTAMP
);
```

### New tables — classification + websites

```sql
classifications (
  company_number   TEXT,
  source_type      TEXT,                    -- 'website' / 'cqc_service_type' / 'cqc_specialism' / 'manual' / 'rule'
  verdict          TEXT NOT NULL,
  verdict_reason   TEXT,
  evidence_quote   TEXT,
  source_url       TEXT,
  classifier       TEXT NOT NULL,           -- 'llm:claude-opus-4-7' / 'rule:keyword' / 'human:user'
  classified_at    TIMESTAMP NOT NULL,
  batch_id         UUID,
  PRIMARY KEY (company_number, source_type)
);

classification_batches (
  batch_id         UUID PRIMARY KEY,
  classifier       TEXT NOT NULL,
  source_type      TEXT NOT NULL,
  started_at       TIMESTAMP NOT NULL,
  finished_at      TIMESTAMP,
  status           TEXT NOT NULL,
  input_count      INTEGER,
  classified_count INTEGER,
  unable_count     INTEGER,
  error_count      INTEGER,
  model_version    TEXT
);

company_websites (
  website_id              INTEGER PRIMARY KEY,
  company_number          TEXT NOT NULL,
  discovered_via          TEXT NOT NULL,    -- 'cqc_provider' / 'cqc_location' / 'web_search' / 'manual'
  source_entity_id        TEXT,             -- providerId or locationId when discovered_via is cqc_*
  url                     TEXT NOT NULL,
  is_primary              BOOLEAN NOT NULL DEFAULT FALSE,
  last_seen_reachable_at  TIMESTAMP,
  discovered_at           TIMESTAMP NOT NULL,
  discovered_by_batch     UUID,
  UNIQUE (company_number, url)              -- same URL once per company
);

-- Enforce at most one primary URL per company
CREATE UNIQUE INDEX idx_one_primary_per_company
  ON company_websites (company_number) WHERE is_primary;
```

A company can hold many rows (a chain with multiple locations + a head-office URL). The application picks `is_primary` per the rule below. The unique partial index makes that invariant enforced, not aspirational.

### New tables — batch / sync tracking

```sql
cqc_sync_batches (
  batch_id         UUID PRIMARY KEY,
  sync_type        TEXT NOT NULL,           -- 'bulk_directory' / 'bulk_hsca' / 'api_providers' / 'api_locations'
  mode             TEXT,                    -- 'all' / 'list' / 'incremental'
  started_at       TIMESTAMP NOT NULL,
  finished_at      TIMESTAMP,
  status           TEXT NOT NULL,
  records_fetched  INTEGER,
  records_updated  INTEGER,
  error_count      INTEGER
);
```

### Views + macros

```sql
-- Macros (tier_engine.py logic ported to SQL)
CREATE MACRO class_score(verdict) AS CASE ... END;
CREATE MACRO age_score(avg_age, over_60, all_60_plus) AS CASE ... END;
CREATE MACRO size_score(revenue) AS CASE ... END;
CREATE MACRO tier_for(total, class_pts) AS CASE ... END;

-- Priority view for classification
CREATE VIEW company_current_classification AS
WITH ranked AS (
  SELECT
    company_number, verdict, source_type, evidence_quote, source_url,
    classified_at, classifier,
    ROW_NUMBER() OVER (
      PARTITION BY company_number
      ORDER BY CASE source_type
        WHEN 'manual'           THEN 1
        WHEN 'website'          THEN 2
        WHEN 'cqc_service_type' THEN 3
      END
    ) AS priority,
    COUNT(DISTINCT verdict) OVER (PARTITION BY company_number) AS distinct_verdicts,
    COUNT(*)                   OVER (PARTITION BY company_number) AS source_count
  FROM classifications
)
SELECT *,
  CASE
    WHEN source_count >= 2 AND distinct_verdicts = 1 THEN 'agree'
    WHEN source_count >= 2 AND distinct_verdicts > 1 THEN 'disagree'
    ELSE 'single_source'
  END AS sources_agreement
FROM ranked WHERE priority = 1;

-- Tiered targets view (the deliverable)
CREATE VIEW tiered_targets AS
SELECT
  c.company_number,
  c.company_name,
  c.postcode,
  c.address_post_town,
  ce.avg_director_age,
  ce.directors_over_60,
  ce.all_directors_60_plus,
  ce.revenue,
  ce.revenue_source,
  cls.verdict,
  cls.source_type AS classification_source,
  cls.evidence_quote,
  cls.source_url,
  cls.sources_agreement,
  m.cqc_provider_id,
  m.status AS match_status,
  m.total_score AS match_score,
  class_score(cls.verdict) AS class_pts,
  age_score(ce.avg_director_age, ce.directors_over_60, ce.all_directors_60_plus) AS age_pts,
  size_score(COALESCE(ce.revenue, 0)) AS size_pts,
  (class_score(cls.verdict)
   + age_score(ce.avg_director_age, ce.directors_over_60, ce.all_directors_60_plus)
   + size_score(COALESCE(ce.revenue, 0))) AS total_score,
  tier_for(total_score, class_pts) AS tier
FROM companies c
LEFT JOIN company_enrichment ce USING (company_number)
LEFT JOIN company_current_classification cls USING (company_number)
LEFT JOIN current_company_match m USING (company_number)
WHERE (c.sic_code_1 = '88100' OR c.sic_code_2 = '88100'
       OR c.sic_code_3 = '88100' OR c.sic_code_4 = '88100')
  AND c.company_number NOT IN (
    SELECT ec.company_number
    FROM excluded_companies ec
    JOIN exclusion_lists el USING (list_name)
    WHERE el.is_active
  );
```

---

## Match scoring — signal table

Shipping with **simplified 2-signal logic** matching the existing tested reconcile.py behavior. Multi-signal scoring deferred until we have a real review queue to tune against.

| Signal | Points | Behavior |
|---|---:|---|
| CH number match (CQC `companies_house_number` = CH `company_number`) | 100 | Auto-confirm |
| Fuzzy current name (rapidfuzz token_set_ratio ≥ 90) + same postcode outward code | 90 | Auto-confirm |
| Below threshold | — | Not written |

Notes:
- `NOISE_TOKENS` for normalization includes `ltd`, `limited`, `plc`, `llp`, `the`, `uk` — **does NOT strip `care` / `services`** (codex flagged this as a false-positive vector in the original code).
- All other signals from the original draft (prev_name, no-postcode, aggressive normalize) are computed and stored in `match_signals` JSON as annotations, but do NOT affect auto-confirm.
- Multi-signal scoring (full weight table) deferred to a later phase once review queue exists.

Thresholds:
- ≥ 90 → `auto_confirmed`
- 80–89 (CH number present but data quality flagged) → `needs_review` (rare)
- < 80 → not written

---

## GUI changes

Existing GUI: `ChBulkApp` with three panes (CH Companies, CQC, Settings). Left nav, content area, bottom progress strip.

### Extend `CQCPane`

Add to existing Download/Process/Sync row:
- "HSCA Sync" button (separate from directory sync — they're different files, different cadence)
- "API Enrich" button → opens sub-panel:
  - Mode toggle: All / From CSV
  - Entity toggle: Providers / Locations / Both
  - Incremental (default) / Force-refresh checkbox

### New pane: `ExcludedPane`

- List view: `exclusion_lists` (name, source, member count, active toggle, last_imported_at)
- Action buttons: Import from CSV, Manual add list, Toggle active
- Drill-down: members of selected list (company_number, name from companies JOIN, reason)
- Special: "Import workbook Excluded tab (one-time)" — runs staging resolver

### New pane: `MatchPane`

- Status panel: total matches, auto_confirmed count, needs_review count, user_confirmed count, user_rejected count
- Action buttons: "Score new pairs" (incremental), "Full rerun" (force)
- Review queue table: needs_review rows with score, signals, both names, both postcodes
- Per-row actions: Confirm / Reject

### New pane: `ClassifyPane`

Combines website + classification (sequential, same mental model).

- Status panel: companies with website / without; classifications by source; disagreements count
- Action buttons:
  - "Seed websites from CQC" (populates `company_websites` from HSCA)
  - "Find missing websites" (DDG search for companies with no website row)
  - "Classify pending" (LLM call per company with primary website but no website-source classification)
  - "Verify classifications" (re-classify; logs disagreements)
- View: `company_current_classification` with filter by source, verdict, agreement

### New pane: `PipelinePane`

- Sanity panel: red/green for each gate (CH bulk loaded / CQC bulk loaded / HSCA bulk loaded / excluded list imported)
- "Run Pipeline" button (runs API enrich → match → seed websites → find missing → classify)
- Per-step force-refresh toggles
- Log pane: live step-by-step status
- On error: halt with clear message + Continue Anyway button

### New pane: `TargetsPane`

- Filters: tier, classification verdict, region, agreement, match_status, revenue band
- Table view: `tiered_targets` with sort
- Action buttons: "Export xlsx", "Export csv"

---

## Implementation plan

Per-step bite-sized. Each step ends at a green build + sanity check.

### Phase 1 — Schema + bulk additions

**Step 1.1** — Add `sql/cqc/bootstrap_hsca_locations.sql`. Schema for `cqc_hsca_locations` (122 cols) + `cqc_hsca_dual_registrations`. Unit test: `CREATE TABLE` then describe.

**Step 1.2** — `ch_bulk/cqc_downloader.py`: add `download_hsca_filters(target_date)` that fetches the latest `*_HSCA_Active_Locations.ods` URL from `https://www.cqc.org.uk/about-us/transparency/using-cqc-data`, saves to `data/input/cqc/hsca_active_locations_<date>.ods`. Unit test: mock the HTTP.

**Step 1.3** — `ch_bulk/cqc_processor.py`: add `process_hsca_filters(file_path, batch_id)`. Uses `odfpy` + `pandas`; bulk INSERT into staging table; sanity check (row count, % populated of `Provider Companies House Number`); upsert into `cqc_hsca_locations`. Integration test: ingest a 50-row sample fixture.

**Step 1.4** — Schemas for the rest: `exclusion_lists`, `excluded_companies`, `excluded_staging`, `ch_cqc_matches`, `company_enrichment`, `cqc_api_responses`, `cqc_providers_enriched`, `cqc_locations_enriched`, `classifications`, `classification_batches`, `company_websites`, `cqc_sync_batches`. All in `sql/bootstrap_pipeline.sql`. Test: `CREATE` + describe.

**Step 1.5** — DuckDB macros (`class_score`, `age_score`, `size_score`, `tier_for`) + `company_current_classification` view + `tiered_targets` view. SQL file `sql/macros_and_views.sql`. Test: insert synthetic data, assert view output matches Python `tier_engine.py` for the same input.

### Phase 2 — CH director enrichment

**Step 2.1** — Port `uk-homecare-deals/scripts/ch_client.py` officer-fetch logic into `ch_bulk/ch_enricher.py`. Rate-limited, resumable, writes to `company_enrichment`. Mode: list of company_numbers (default = all SIC-88100 with NULL `last_enriched_at`).

**Step 2.2** — Port revenue model (`uk-homecare-deals/scripts/revenue_model.py` + bands CSV). Writes `revenue` / `revenue_source` columns of `company_enrichment` for blanks.

**Step 2.3** — CLI: `ch-bulk enrich companies --sic 88100`. Test against fixture officer responses.

### Phase 3 — CQC API enrichment

**Step 3.1** — `ch_bulk/cqc_api_client.py`. Methods: `get_provider(provider_id)`, `get_location(location_id)`. Auth via env var `CQC_SUBSCRIPTION_KEY`. Throttled (2000 req/min, with 429 backoff).

**Step 3.2** — `ch_bulk/cqc_api_enricher.py`. Reads `cqc_providers` / `cqc_locations` for pending IDs (mode: all / list / incremental). Calls API, writes raw JSON to `cqc_api_responses`, parses into `cqc_*_enriched`. Resumable via `cqc_sync_batches`.

**Step 3.3** — CLI: `ch-bulk cqc-enrich providers --mode incremental`, same for locations. Test against recorded API fixtures.

### Phase 4 — Matching

**Step 4.1** — `ch_bulk/matcher.py`. Multi-signal scoring per the table above. Inputs: `companies`, `cqc_providers` + `cqc_hsca_locations` (for `companies_house_number` from bulk), `cqc_providers_enriched` (for CH number from API). Output: rows for `ch_cqc_matches`.

**Step 4.2** — Two modes: incremental (only new pairs since last_seen) + full rerun (recompute all, preserve user_confirmed/user_rejected).

**Step 4.3** — CLI: `ch-bulk match`. Tests: known-match fixtures, known-near-miss (the "ABC Care" vs "ABC Cars" trap), prev_name match.

### Phase 5 — Excluded list

**Step 5.1** — `ch_bulk/excluded_importer.py`. Loads workbook Excluded tab into `excluded_staging`. Resolver: exact name+postcode → `companies`, then fuzzy. Outputs three buckets (matched, needs_review, unmatched).

**Step 5.2** — Confirm step: commits resolved rows into `excluded_companies` under list_name `workbook-import`.

**Step 5.3** — CLI: `ch-bulk excluded import --workbook <path>`, `ch-bulk excluded list`, `ch-bulk excluded toggle <list_name>`. Tests: known-tab fixture, fuzzy edge cases.

### Phase 6 — Websites

**Step 6.1** — `ch_bulk/website_seeder.py`. Reads `cqc_hsca_locations.provider_web_address` + `location_web_address`, joins via match, writes to `company_websites` with `discovered_via='cqc_provider'/'cqc_location'`.

**Step 6.2** — Port `uk-homecare-deals/scripts/web_search.py` + `find_websites.py` into `ch_bulk/website_finder.py`. Runs only against companies with no row in `company_websites` from `cqc_*` discovered_via sources.

**Step 6.3** — Primary-URL rule (manual > most-recent-reachable > first-discovered) implemented as a SQL UPDATE after each seed/find run.

**Step 6.4** — CLI: `ch-bulk websites seed`, `ch-bulk websites find-missing`.

### Phase 7 — Classification

**Step 7.1** — `ch_bulk/classifier.py`. Per-company: fetch primary URL (use `requests` first, fall back to Playwright if empty body), call LLM via OpenAI-compatible HTTP API (default: Ollama at `http://localhost:11434/v1` with model `qwen2.5:7b` or similar). Parse JSON response (verdict + evidence + verdict_reason), write to `classifications` (source_type=`website`, classifier=`llm:<model_id>`). Config: `llm_provider` / `llm_model` / `llm_endpoint` keys in settings so we can swap to Anthropic / OpenAI later without code change.

**Step 7.2** — CQC-derived classifier: read HSCA `Service type - Domiciliary care service`, `Service type - Care home service with nursing`, etc. flags; derive `cqc_service_type` source classification. Pure SQL; no API call.

**Step 7.3** — Updates `company_websites.last_seen_reachable_at` whenever the website fetch succeeds.

**Step 7.4** — CLI: `ch-bulk classify --source website|cqc_service_type --mode incremental|verify`.

### Phase 8 — Export

**Step 8.1** — `ch_bulk/exporter.py`. Multi-tab xlsx (Summary, Tier 1, Tier 2, Tier 3, Excluded, Needs Review, Disagreements). Single-CSV variant.

**Step 8.2** — CLI: `ch-bulk export xlsx --out <path>`, `ch-bulk export csv --out <path>`.

### Phase 9 — GUI

**Step 9.1** — Extend `CQCPane`: HSCA Sync button, API Enrich sub-panel.

**Step 9.2** — `ExcludedPane`: list view + members drill-down + import flow.

**Step 9.3** — `MatchPane`: status + review queue + confirm/reject actions.

**Step 9.4** — `ClassifyPane`: combined websites + classification with status + actions.

**Step 9.5** — `PipelinePane`: sanity panel + Run Pipeline + per-step toggles + log pane.

**Step 9.6** — `TargetsPane`: filters + table + export buttons.

**Step 9.7** — Wire new panes into `ChBulkApp._build_ui`.

### Phase 10 — End-to-end

**Step 10.1** — Run full pipeline against real bulk data + workbook excluded import. Verify tier distribution sensibility (no Tier 1 = 600 rows).

**Step 10.2** — Side-by-side compare: `tiered_targets` view output vs current `uk-homecare-deals` `tiered_targets.xlsx` for a sample of 50 companies. Investigate any tier disagreement.

**Step 10.3** — Document migration: how the user moves from running uk-homecare-deals to running ch_scraper for this workflow.

---

## Open questions for review

1. **Tier engine view performance** — at 11K companies the LEFT JOINs and window functions in `company_current_classification` are trivial. But if we ever go national (millions of CH companies), the view becomes expensive. Materialized view + manual refresh button vs always-live view? Currently going with always-live; flag for review.

2. **LLM classification scope** — running Ollama locally so per-call cost is zero. But classification runs **only against a filtered subset** of financially-viable companies (revenue band, employee count, has CQC match) — not all 11K. Filter logic lives as a saved view `classification_candidates` and is refreshable. Subset size estimated 500-1500. Ollama at ~1-2 sec per company = 10-30 min unattended.

3. **Match scoring weights** — pulled the numbers (60/50/40/35/...) out of judgment. Worth back-testing against the user's existing manual matches before locking?

4. **HSCA bulk file is monthly, CQC directory is weekly, CH bulk is monthly** — but the directory has Provider/Location IDs we use for the API. If she syncs the directory weekly but HSCA only monthly, the IDs in directory may have entries with no matching HSCA row. Handle gracefully (left join, fallback to API on miss)?

5. **Manual classification entry path** — does the GUI need a "manually classify company X as Y" affordance, or does she just edit the row in SQL / via a script? Adding manual entry as a UI is straightforward but I haven't drawn it.

6. **Backwards compat with uk-homecare-deals** — do we deprecate that project once this works, or maintain it in parallel? She's used to its workflow. Recommend: keep uk-homecare-deals for 1-2 weeks of parallel verification, then deprecate.

---

## Appendix — classification LLM prompt template

```
Classify this UK homecare company's website into one of:
- Majority domiciliary
- Majority Supported living
- Majority residential
- Mixed_domiciliary_supported
- Mixed_residential_domiciliary
- Unable to classify - <reason>
- Excluded - non-homecare

Definitions:
- Domiciliary = care delivered in the client's own home (visiting care, live-in)
- Supported living = clients live in their own tenancy with on-site/visiting support, typically learning disabilities
- Residential = care home where clients live full-time

Site content:
<extracted text from URL>

Return JSON: {"verdict": "<one of above>", "evidence": "<one quote justifying the verdict>", "verdict_reason": "<optional reason for Unable/Excluded>"}
```

Strict JSON parse; fail → `Unable to classify - parse_error`.
