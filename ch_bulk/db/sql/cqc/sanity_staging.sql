-- Sanity check: duplicates in cqc_locations_staging.
-- Strict zero. Force does NOT override.

WITH dups AS (
    SELECT location_id, COUNT(*) AS n
    FROM cqc_locations_staging
    GROUP BY location_id
    HAVING COUNT(*) > 1
),
sample AS (
    SELECT * FROM dups ORDER BY n DESC LIMIT 5
)
SELECT
    (SELECT COUNT(*)                FROM dups)    AS dup_distinct_ids,
    (SELECT COALESCE(SUM(n - 1), 0) FROM dups)    AS dup_excess_rows,
    (SELECT LIST(location_id)       FROM sample)  AS dup_sample;
