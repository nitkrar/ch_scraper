-- Upsert (inverted model): rebuild companies table from staging,
-- carrying forward orphan rows from old companies as inactive.
--
-- Existing-rows path. Runs ONLY when `companies` already exists.
-- The {{scrape_date}} placeholder is substituted by the Python
-- orchestrator before execution.
--
-- Rationale: full-table rebuild from staging is ~3 min on this data
-- (close to the original DROP+CREATE baseline) because every row is
-- written sequentially with no index lookups or page churn. We then
-- carry forward the orphan rows (in old `companies` but not in
-- staging) as a separate INSERT, marking them inactive. Total cost
-- ≈ original DROP+CREATE + ~5% (orphan insert is tiny since orphans
-- are a small fraction of the table).
--
-- This is faster than MERGE/UPDATE-in-place because DuckDB writes
-- sequentially to a new table instead of random-accessing 5.7M rows
-- across the columnar storage to update them in place.
--
-- Three logical row sources go into companies_new:
--   1. MATCHED (in both staging and companies):
--      → take all data columns from staging
--      → preserve first_scrape_date + last_enriched_at from companies
--      → set last_scrape_date = scrape, is_active = TRUE, marked_inactive_scrape_date = NULL
--   2. NEW (in staging only):
--      → take all data columns from staging
--      → first_scrape_date = last_scrape_date = scrape
--      → is_active = TRUE
--   3. ORPHAN (in companies only):
--      → take everything from companies (including its existing tracking)
--      → force is_active = FALSE
--      → set marked_inactive_scrape_date = COALESCE(existing, scrape)
--        so rows already marked inactive keep their original date
--
-- Final step: DROP old companies, RENAME companies_new → companies.
-- Indexes are recreated by indexes.sql in the next orchestrator step.
--
-- Run standalone (replace {{scrape_date}} first):
--   $ sed "s/{{scrape_date}}/2026-05-01/g" sql/ch/upsert_companies.sql \
--       | duckdb ch_bulk.duckdb

DROP TABLE IF EXISTS companies_new;

-- Create empty companies_new with the same schema as companies
CREATE TABLE companies_new AS
SELECT * FROM companies WHERE FALSE;

-- (1) Matched rows — staging data + preserved tracking from old companies
INSERT INTO companies_new BY NAME
SELECT
    s.*,
    c.first_scrape_date,
    DATE '{{scrape_date}}'   AS last_scrape_date,
    TRUE                     AS is_active,
    CAST(NULL AS DATE)       AS marked_inactive_scrape_date,
    c.last_enriched_at
FROM companies_staging AS s
JOIN companies         AS c ON c.company_number = s.company_number;

-- (2) New rows — in staging, not previously in companies
INSERT INTO companies_new BY NAME
SELECT
    s.*,
    DATE '{{scrape_date}}'   AS first_scrape_date,
    DATE '{{scrape_date}}'   AS last_scrape_date,
    TRUE                     AS is_active,
    CAST(NULL AS DATE)       AS marked_inactive_scrape_date,
    CAST(NULL AS TIMESTAMP)  AS last_enriched_at
FROM companies_staging AS s
WHERE NOT EXISTS (
    SELECT 1 FROM companies c WHERE c.company_number = s.company_number
);

-- (3) Orphan rows — in old companies, not in staging. Carry forward,
-- mark inactive (or preserve existing marked_inactive_scrape_date).
INSERT INTO companies_new BY NAME
SELECT
    c.* EXCLUDE (is_active, marked_inactive_scrape_date),
    FALSE                                                          AS is_active,
    COALESCE(c.marked_inactive_scrape_date, DATE '{{scrape_date}}') AS marked_inactive_scrape_date
FROM companies AS c
WHERE NOT EXISTS (
    SELECT 1 FROM companies_staging s WHERE s.company_number = c.company_number
);

-- Atomic swap (inside the transaction the orchestrator wraps around this file)
DROP TABLE companies;
ALTER TABLE companies_new RENAME TO companies;
