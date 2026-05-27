-- Upsert (inverted model): rebuild cqc_locations from staging,
-- carrying forward orphan rows from old table as inactive.
--
-- Same pattern as sql/ch/upsert_companies.sql — see comments there
-- for rationale. The {{scrape_date}} placeholder is substituted by
-- the Python orchestrator.
--
-- Match key: location_id (CQC's per-location unique ID).

DROP TABLE IF EXISTS cqc_locations_new;

CREATE TABLE cqc_locations_new AS
SELECT * FROM cqc_locations WHERE FALSE;

-- (1) Matched rows: staging data + preserved tracking
INSERT INTO cqc_locations_new BY NAME
SELECT
    s.*,
    c.first_scrape_date,
    DATE '{{scrape_date}}'   AS last_scrape_date,
    TRUE                     AS is_active,
    CAST(NULL AS DATE)       AS marked_inactive_scrape_date,
    c.last_enriched_at
FROM cqc_locations_staging AS s
JOIN cqc_locations         AS c ON c.location_id = s.location_id;

-- (2) New rows: in staging, not previously in cqc_locations
INSERT INTO cqc_locations_new BY NAME
SELECT
    s.*,
    DATE '{{scrape_date}}'   AS first_scrape_date,
    DATE '{{scrape_date}}'   AS last_scrape_date,
    TRUE                     AS is_active,
    CAST(NULL AS DATE)       AS marked_inactive_scrape_date,
    CAST(NULL AS TIMESTAMP)  AS last_enriched_at
FROM cqc_locations_staging AS s
WHERE NOT EXISTS (
    SELECT 1 FROM cqc_locations c WHERE c.location_id = s.location_id
);

-- (3) Orphan rows: in old, not in staging — carry forward as inactive
INSERT INTO cqc_locations_new BY NAME
SELECT
    c.* EXCLUDE (is_active, marked_inactive_scrape_date),
    FALSE                                                          AS is_active,
    COALESCE(c.marked_inactive_scrape_date, DATE '{{scrape_date}}') AS marked_inactive_scrape_date
FROM cqc_locations AS c
WHERE NOT EXISTS (
    SELECT 1 FROM cqc_locations_staging s WHERE s.location_id = c.location_id
);

DROP TABLE cqc_locations;
ALTER TABLE cqc_locations_new RENAME TO cqc_locations;
