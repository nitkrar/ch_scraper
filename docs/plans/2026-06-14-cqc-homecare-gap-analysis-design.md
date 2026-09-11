# CQC Homecare Gap Analysis — Design

**Date:** 2026-06-14
**Input workbook:** `/Users/nitinkum/Downloads/13062026_Potentially_missing_homecare_-_CQC_data.xlsx`

## Source-of-truth notes

- Task 1 input sheet is `Potentially missing homecare`.
- `Potentially missing homecare` has `2,678` worksheet data rows, but only `873` populated provider rows and `1,805` blank rows. Filter to populated rows at runtime.
- Those `873` populated rows contain `873` provider IDs, `873` provider names, `873` populated postcodes (`860` distinct), and `29` distinct service-type combinations.
- The most common service-type combinations in those `873` rows are:
  - `Homecare agencies`: `665`
  - `Homecare agencies|Supported living`: `136`
  - `Residential homes|Homecare agencies`: `16`
  - `Homecare agencies|Supported housing`: `11`
- `database` has `16,539` data rows.

## Relevant DuckDB state

- `companies`: `5,765,115` rows
- `ch_cqc_matches`: `16,538` rows
- `company_enrichment`: `16,679` rows
- `tiered_targets`: `16,530` rows
- `cqc_providers`: `36,939` rows
- `cqc_hsca_locations`: `56,815` rows

The workbook `database` tab is therefore not a clean mirror of `tiered_targets`. Treat Task 2 as a workbook/output correction unless the user explicitly wants the screening model changed in DuckDB as a second step.

---

## Task 1 — Find independent homecare providers missing from the workbook database tab

### Step 1.0 — Use the right input grain

Use `Potentially missing homecare` as the canonical Task 1 input. Filter to the `873` populated rows, then treat those rows as the provider-level working set.

Each populated row includes:

- `Provider name`
- `CQC Provider ID (for office use only)`
- `Postcode`
- `Service types`

Task 1 cross-checks against the workbook `database` tab only. No other workbook sheet is involved in Task 1.

### Step 1.1 — Filter non-independent entities using CQC signals first

Because the populated Task 1 rows already give `provider_id`, the first pass should join to:

- `cqc_providers`
- `cqc_providers_enriched`
- `cqc_hsca_locations`

Observed coverage on the `873` populated rows:

- `864 / 873` provider IDs are present in `cqc_providers`
- `860 / 873` have populated `ownership_type`
- `52 / 873` have populated `brand_name`
- `49 / 873` have populated `charity_number`

Apply these filters before any fuzzy Companies House search:

1. Exclude obvious non-company / non-target ownership types from `cqc_providers_enriched.ownership_type`:
   - `Individual`
   - `Partnership`
   - `NHS Body`
2. Exclude likely charities / not-for-profits when either of these is present:
   - `cqc_providers_enriched.charity_number`
   - provider / company name signals such as `charity`, `trust`, `council`, `borough council`, `county council`, `NHS`
3. Treat `brand_name` / `provider_brand_name` as the primary franchise-or-chain signal.
   - Do not rely on a hard-coded name list alone.
   - Keep a small manual override list for false positives / false negatives.
4. Only after provider-level CQC filtering, resolve the Companies House entity and inspect `companies.company_type`.

### Step 1.2 — Resolve Companies House numbers in priority order

Use the existing identifiers before inventing a new CH search path:

1. `cqc_providers_enriched.companies_house_number`
   - Present for `504 / 873` populated Task 1 rows.
2. `cqc_hsca_locations.provider_companies_house_number`
   - Present for `494 / 873` populated Task 1 rows.
3. Local `companies` table exact/normalized lookup by name + postcode.
4. Local fuzzy lookup by normalized name constrained by postcode/outward postcode.
5. Optional Companies House API search only for the unresolved remainder.

Do **not** claim an existing CH search helper in `ch_bulk/companies_house/query.py`; that module is local DuckDB query code, not a Companies House search client.

### Step 1.3 — Apply company-type screening after CH number resolution

Exclude these `companies.company_type` values by default:

| Exclude `company_type` | Reason |
|---|---|
| `Registered Society` | society / mutual |
| `Community Interest Company` | CIC / non-profit |
| `Charitable Incorporated Organisation` | charity |
| `Scottish Charitable Incorporated Organisation` | charity |
| `PRI/LTD BY GUAR/NSC (...)` | guarantee structure, commonly non-profit |
| `PRI/LBG/NSC (...)` | guarantee structure / limited exemption |
| `Royal Charter Company` | charter body |
| `Industrial and Provident Society` | society / mutual |
| `Other company type` / `Other Company Type` | ambiguous, review manually |
| `Overseas Entity` | not the intended UK operating-company target |

Auto-pass:

- `Private Limited Company`
- `Limited Liability Partnership`

Manual review instead of auto-pass:

- `Private Unlimited Company`
- `Limited Partnership`
- `Public Limited Company`

This is stricter than the first draft and is intentional: the dataset is small enough that rare commercial forms can sit in a review queue.

### Step 1.4 — Cross-check against the workbook `database` tab

Match against the workbook `database` tab in this order:

1. Normalized company number
   - zero-pad numeric-looking values to 8 chars before comparing
2. Exact normalized company name + normalized postcode
3. Fuzzy name match only as a manual-review path

Notes:

- The workbook contains at least one stripped leading zero in `Company Number` (`7885189` vs `07885189` in DuckDB), so raw Excel values are not safe join keys.
- The workbook also contains rows with blank/invalid company numbers, so Task 1 needs `match_source` and `needs_manual_review` columns.
- Keep the Task 1 workbook scope narrow: `Potentially missing homecare` -> `database` only.

### Step 1.5 — Output columns for Task 1

For each provider row, output:

- `provider_id`
- `provider_name`
- `postcode`
- `ownership_type`
- `brand_name`
- `charity_number`
- `candidate_company_number`
- `candidate_company_name`
- `company_type`
- `match_source` (`cqc_api`, `hsca`, `local_exact`, `local_fuzzy`, `manual`)
- `independence_decision` (`include`, `exclude`, `review`)
- `decision_reason`
- `already_in_database`
- `database_row_tier`

**No financials scraping** is required for this task.

---

## Task 2 — Reclassify wrongly-excluded domiciliary rows in the workbook `database` tab

### Current state in the workbook

In the workbook `database` tab:

- `9,567` rows have `Claude Tier = Excluded`
- `2,696` of those have `Classification in use = Domiciliary Care`
- `1,018` of those have `Classification in use = Domiciliary Care + Supported Living`

This task should operate on the workbook rows directly. Do not assume the same rows can be updated safely via `tiered_targets`.

### Step 2.1 — Reclassification rules

| `Classification in use` | New result |
|---|---|
| `Domiciliary Care` | `Tier 1` if oldest director >= 60 or `directors_over_60 > 0`; else `Tier 2` |
| `Domiciliary Care + Supported Living` | `Tier 3` |

### Step 2.2 — Director-age source of truth

Use these inputs in order:

1. Workbook column `P` (`Oldest Director`)
2. `company_enrichment.max_director_age`
3. `company_enrichment.directors_over_60`
4. `company_enrichment.all_directors_60_plus`

Data-quality note:

- `18` excluded `Domiciliary Care` rows have no workbook `Oldest Director`
- `1` excluded `Domiciliary Care + Supported Living` row has no workbook `Oldest Director`

So a DuckDB fallback is required even for an Excel-only deliverable.

### Step 2.3 — Non-residential nursing check

Use DuckDB as the primary source, not the raw workbook tabs:

1. Resolve `company_number` from the workbook row, normalizing to 8 chars where needed.
2. Join through `current_company_match` / `ch_cqc_matches` to get `cqc_provider_id` when available.
3. Query `cqc_locations.service_types` for explicit non-residential nursing signals.

Primary positive signal:

- `Community services - Nursing`

Do **not** treat every `nursing` substring as positive, because `Nursing homes` is residential care and would create false positives.

Use workbook sheets `CQC` / `Sheet3` only as human-audit references when a DuckDB-backed join fails.

### Step 2.4 — Output columns for Task 2

For each reclassified row, output:

- original workbook row key
- normalized `company_number`
- `company_name`
- original `Claude Tier`
- `Classification in use`
- `oldest_director`
- `directors_over_60`
- `all_directors_60_plus`
- `new_tier`
- `has_non_residential_nursing`
- `tier_reason`
- `data_quality_note`

---

## Deliverables

Produce a new Excel output rather than mutating the source workbook or DuckDB in v1.

Suggested tabs:

| Tab | Purpose |
|---|---|
| `task1_filtered_missing` | Provider-level inclusion/exclusion decision + CH number resolution |
| `task1_review_queue` | Fuzzy matches, rare company types, ambiguous franchise cases |
| `task2_reclassified` | Workbook excluded rows with new tier + nursing flag |
| `summary` | Counts by decision source / tier / unresolved rows |

Optional sidecar CSVs are fine, but the main deliverable should stay workbook-first.

---

## Implementation approach

Python script: `scripts/cqc_homecare_gap.py`

1. Load workbook sheets:
   - `Potentially missing homecare`
   - `database`
2. Normalize identifiers:
   - provider IDs as strings
   - company numbers zero-padded to 8 chars
   - uppercased no-space postcodes
   - normalized company/provider names
3. Filter `Potentially missing homecare` to the `873` populated provider rows before any joins.
4. Join Task 1 rows to `cqc_providers_enriched` and `cqc_hsca_locations` first.
5. Cross-check Task 1 candidates against the workbook `database` tab only.
6. Resolve remaining CH numbers from local DuckDB exact matches.
7. Send fuzzy-only CH matches to a review queue instead of auto-accepting them.
8. Reclassify Task 2 rows directly from the workbook, with `company_enrichment` fallback for missing age fields.
9. Write a new Excel workbook with review-friendly audit columns.

---

## Concerns to resolve before implementation

1. **Franchise detection**
   - A static brand-name list is not robust enough on its own.
   - Use `brand_name` / `provider_brand_name` first, then a curated exception list.

2. **Company-type completeness**
   - The initial exclusion list was too narrow.
   - Rare company types should default to review rather than silent pass.

3. **Fuzzy matching reliability**
   - Fuzzy CH lookup should never silently decide inclusion for ambiguous rows.
   - Keep a review queue for `local_fuzzy` matches.

4. **Workbook vs DuckDB mismatch**
   - The workbook `database` tab has `16,539` rows, while `tiered_targets` has `16,530`.
   - Keep the first implementation export-only unless the user explicitly wants a second pass that changes SQL scoring / exports.

5. **DuckDB write scope**
   - This task does not need a schema change.
   - Default to a new workbook output, not direct DB updates.
