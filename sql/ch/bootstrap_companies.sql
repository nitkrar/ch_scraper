-- Bootstrap: create `companies` from `companies_staging`
--
-- First-time path. Runs ONLY when the `companies` table doesn't exist
-- (the orchestrator checks this and routes accordingly).
--
-- The tracking columns are initialised from the {{scrape_date}}
-- placeholder, substituted by the Python orchestrator before exec.
--
-- Uniqueness is enforced by the UNIQUE INDEX in indexes.sql (built after
-- this file), backstopping the upfront duplicate sanity check.
--
-- Run standalone (replace {{scrape_date}} with the YYYY-MM-DD first):
--   $ sed "s/{{scrape_date}}/2026-04-01/g" sql/ch/bootstrap_companies.sql \
--       | duckdb ch_bulk.duckdb

CREATE TABLE companies AS
SELECT
    *,
    TRUE                            AS is_active,
    DATE '{{scrape_date}}'          AS first_scrape_date,
    DATE '{{scrape_date}}'          AS last_scrape_date,
    CAST(NULL AS DATE)              AS marked_inactive_scrape_date,
    CAST(NULL AS TIMESTAMP)         AS last_enriched_at
FROM companies_staging;
