-- Sanity check: duplicates in companies_staging.
--
-- STRICT ZERO. Any duplicates mean the source CSV has a real data
-- problem — the orchestrator aborts and `force=True` does NOT override.
--
-- Only references companies_staging, so this is safe to run on both
-- the bootstrap path (companies doesn't exist yet) and the upsert path.
--
-- Run standalone:
--   $ duckdb ch_bulk.duckdb
--   D .read sql/ch/sanity_staging.sql

WITH dups AS (
    SELECT company_number, COUNT(*) AS n
    FROM companies_staging
    GROUP BY company_number
    HAVING COUNT(*) > 1
),
sample AS (
    SELECT * FROM dups ORDER BY n DESC LIMIT 5
)
SELECT
    (SELECT COUNT(*)        FROM dups)                            AS dup_distinct_numbers,
    (SELECT COALESCE(SUM(n - 1), 0) FROM dups)                    AS dup_excess_rows,
    (SELECT LIST(company_number) FROM sample)                     AS dup_sample;
