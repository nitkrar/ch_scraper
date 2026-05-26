-- Bootstrap: create `cqc_locations` table from `cqc_locations_staging`
--
-- First-time path. Runs ONLY when `cqc_locations` doesn't exist.
-- The {{scrape_date}} placeholder is substituted by the Python
-- orchestrator before execution.

CREATE TABLE cqc_locations AS
SELECT
    *,
    TRUE                            AS is_active,
    DATE '{{scrape_date}}'          AS first_scrape_date,
    DATE '{{scrape_date}}'          AS last_scrape_date,
    CAST(NULL AS DATE)              AS marked_inactive_scrape_date,
    CAST(NULL AS TIMESTAMP)         AS last_enriched_at
FROM cqc_locations_staging;
