# Upload List → Standard Company Profile — Design

Topic: Desktop-app flow that takes a CSV/Excel of Companies House numbers
(no filters) and produces one standard sheet per company
Status: Milestones 1–2 implemented; milestone 3 proposed

## Goal

A non-technical user of the desktop app picks a file of company numbers and
gets back a fixed-format workbook. The pipeline works on a fresh install with
no bulk Companies House data.

## Milestones

1. **Upload list (this design)** — upload → standard sheet with the fields the
   pipeline already captures.
2. **Balance-sheet fields** — add Debtors, Creditors < 1 yr, Creditors > 1 yr,
   Provisions (incl. deferred tax); stop using `TotalAssetsLessCurrentLiabilities`
   as a Net Assets fallback.
3. **Scanned PDFs** — tesseract OCR + regex extraction, scored against prior
   AI extractions.

## Output columns (milestone 1)

| Column | Source |
|---|---|
| Company Number | input, normalised |
| Company Name, Incorporation Date, Company Status | `companies` |
| Directors | `company_enrichment.directors` → `"Name (age); …"` |
| Total Active Directors, Directors 60+ | `company_enrichment` |
| Min / Avg / Max Director Age | `company_enrichment` |
| Employees | `employee_count` |
| Current Assets, Total Assets, Net Assets | `company_enrichment` |
| Revenue, Profit Before Tax | `company_enrichment`; blank when not filed |
| Accounts Period End, Accounts Source | `filing_period_end`, `filing_format` |
| Notes | why values are blank: `invalid_number`, `not_found` (profile 404), `no_directors_dob`, and from `revenue_source`: `no_recent_filing` → no accounts filed, `partial_no_revenue` → P&L not filed, `pdf_no_text_layer` → scanned PDF, pending OCR |

Latest filed accounts only. Output format: `.xlsx`.

## Decisions

1. **Entry point** — a new left-nav pane **Upload List** in `gui.py`: pick
   file → detect company-number column (header match, else first column whose
   values look like CH numbers) → preview count → Run → save dialog. Runs as a
   cancellable task via `_register_task`. The file picker defaults to `.xlsx`
   (CSV also accepted). A **Download template** button saves
   `company_list_template.xlsx`: one sheet, header `Company Number`, that
   column formatted as text so Excel keeps leading zeros, one example row.
2. **Normalisation** — strip whitespace, upper-case, left-pad digits to 8
   characters keeping alpha prefixes (`SC`, `NI`, `OC`, …). Excel numeric cells
   (leading zeros lost) are padded. Duplicates are removed, order preserved;
   invalid values are reported as `invalid_number` rows in the output.
3. **Missing companies are fetched and loaded** — numbers absent from
   `companies` are looked up via `GET /company/{number}` and inserted into
   `companies` with `record_source = 'api'` and `last_scrape_date` set to
   the fetch day (`first_scrape_date` stays NULL until a bulk snapshot
   includes the company). A 404 produces a `not_found` output row and no
   insert.
4. **No bulk DB** — if `companies` has no bulk rows, the pane prompts
   "Download Companies House bulk data first?". Yes → existing `sync` runs,
   then the upload runs. No → API-only.
5. **`companies.record_source`** (new, nullable TEXT) — `'api'` for upload
   inserts, NULL for bulk rows. Added by `ensure_*` migration on existing DBs.
   `sanity_upsert.sql` counts only `record_source IS NULL` rows, so a DB
   seeded by uploads passes the first bulk process without `force`. Bulk
   refresh behaviour is unchanged otherwise: an API row present in the
   snapshot is overwritten by bulk data (source becomes NULL); one absent is
   carried forward as inactive.
6. **API-only schema parity** — when `companies` does not exist, it is created
   by running the existing staging ingest over a header-only bulk CSV and
   `bootstrap_companies.sql`, so its columns match a bulk-built table exactly.
7. **Profile → companies mapping** — `company_name`, `company_status`,
   `incorporation_date` (`date_of_creation`), `dissolution_date`,
   `sic_code_1..4`, registered address fields, `accounts_last_made_up`,
   `accounts_next_due`, `accounts_ref_day/month`, `conf_stmt_*`. Status and
   category are stored in bulk vocabulary (`active` → `Active`); unmapped
   columns stay NULL. `is_active` follows status.
8. **Reuse vs refresh** — default reuses stored data less than a month old.
   A profile is re-fetched when `companies.last_scrape_date` is more than a
   month old or NULL; the row is updated in place, keeping bulk-only columns
   and its `record_source`. Directors and financials share
   `company_enrichment.last_enriched_at`: more than a month old → both are
   fetched again. Otherwise directors are fetched only when
   `directors_dob_years IS NULL`, and financials only when no filed accounts
   are stored. Targets are chosen before any fetch, because a directors
   fetch stamps `last_enriched_at`. A **Refresh from Companies House**
   checkbox forces directors and financials for every company and the
   profile for API rows.
   Existing `enrich_directors(company_numbers=…)` and
   `enrich_financials(mode="list")` do the fetching.
9. **Per-director age** — `compute_director_meta` adds `date_of_birth`
   (`{"month", "year"}`) to each entry in the `directors` JSON. Existing rows
   are rebuilt from the stored officer responses (`cqc_api_responses`,
   `entity_type = 'ch_directors'`) via `backfill_directors`. Age =
   current year − birth year, consistent with the existing min/avg/max.

## Milestone 2 — balance-sheet fields (implemented)

1. **New iXBRL fields** — Debtors, Creditors within / after one year,
   Provisions for liabilities, Deferred tax. Creditors are split by the
   maturity or current/non-current dimension (namespace prefix ignored); an
   undimensioned `Creditors` total is not used. Creditors and provisions are
   stored as positive amounts; debtors and deferred tax keep their sign.
2. **Latest year only** — balance-sheet values come only from facts dated at
   the filing's balance-sheet date; a prior-year comparative is never used.
   Net assets no longer falls back to `TotalAssetsLessCurrentLiabilities`.
3. **A newly parsed filing replaces all its figures** — blanks included, so no
   older filing's figure (or a revenue estimate) survives beside it. Fetch
   errors and "no filing" results keep today's fill-the-gaps merge; a re-fetch
   of a scanned PDF whose figures were recovered by OCR keeps the stored row.
4. **Headcount** — a headcount tagged with a negative scale (108 shown, 1.08
   parsed) uses the displayed whole number.
5. **Untagged turnover** — when no revenue fact is tagged, a printed
   `Turnover`/`Revenue` row with current- and prior-year amounts is used; the
   largest matching amount wins (a £000 summary table can precede the P&L).
6. Text PDFs do not yet yield the new fields. Rows stored before this change
   get them only when their filing is re-read.

## Out of scope

OCR of scanned PDFs (milestone 3).
