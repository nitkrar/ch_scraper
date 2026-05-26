-- Provider-level rollup from cqc_locations.
--
-- One row per provider_id. List columns hold the DISTINCT values seen
-- across that provider's ACTIVE locations:
--
--   service_types_list:        ['Homecare agencies', 'Care home service']
--   regions_list:              ['London']
--   postcode_prefixes_list:    ['SW1A', 'EC1A', 'M1']
--   local_authorities_list:    ['Westminster']
--
-- `active_location_count` is exposed as a column so "providers
-- operating > N locations" is a trivial filter without inflating
-- the list columns with sentinel values.
--
-- Recomputed (DROP + CREATE) after every upsert_locations.sql run.

DROP TABLE IF EXISTS cqc_providers;

CREATE TABLE cqc_providers AS
WITH
    services_unnested AS (
        SELECT
            provider_id,
            is_active,
            trim(unnest(string_split(service_types, '|'))) AS service
        FROM cqc_locations
        WHERE service_types IS NOT NULL AND service_types != ''
    ),
    service_lists AS (
        SELECT
            provider_id,
            LIST(DISTINCT service) AS service_types_list
        FROM services_unnested
        WHERE is_active AND service IS NOT NULL AND service != ''
        GROUP BY provider_id
    ),

    postcode_lists AS (
        SELECT
            provider_id,
            LIST(DISTINCT regexp_extract(upper(trim(postcode)),
                                          '^([A-Z]{1,2}[0-9][A-Z0-9]?)', 1))
                AS postcode_prefixes_list
        FROM cqc_locations
        WHERE is_active AND postcode IS NOT NULL AND postcode != ''
        GROUP BY provider_id
    ),

    region_lists AS (
        SELECT
            provider_id,
            LIST(DISTINCT region) AS regions_list
        FROM cqc_locations
        WHERE is_active AND region IS NOT NULL AND region != ''
        GROUP BY provider_id
    ),

    la_lists AS (
        SELECT
            provider_id,
            LIST(DISTINCT local_authority) AS local_authorities_list
        FROM cqc_locations
        WHERE is_active AND local_authority IS NOT NULL AND local_authority != ''
        GROUP BY provider_id
    ),

    base AS (
        SELECT
            provider_id,
            mode(provider_name)                       AS provider_name,
            COUNT(*) FILTER (WHERE is_active)         AS active_location_count,
            COUNT(*)                                  AS total_location_count,
            MIN(first_scrape_date)                    AS first_scrape_date,
            MAX(last_scrape_date)                     AS last_scrape_date,
            BOOL_OR(is_active)                        AS is_active,
            CASE WHEN BOOL_OR(is_active) THEN NULL
                 ELSE MAX(marked_inactive_scrape_date) END AS marked_inactive_scrape_date
        FROM cqc_locations
        GROUP BY provider_id
    )
SELECT
    b.provider_id,
    b.provider_name,
    b.active_location_count,
    b.total_location_count,
    COALESCE(sl.service_types_list,     CAST([] AS VARCHAR[])) AS service_types_list,
    COALESCE(rl.regions_list,           CAST([] AS VARCHAR[])) AS regions_list,
    COALESCE(pl.postcode_prefixes_list, CAST([] AS VARCHAR[])) AS postcode_prefixes_list,
    COALESCE(ll.local_authorities_list, CAST([] AS VARCHAR[])) AS local_authorities_list,
    b.first_scrape_date,
    b.last_scrape_date,
    b.is_active,
    b.marked_inactive_scrape_date
FROM base                  b
LEFT JOIN service_lists  sl USING (provider_id)
LEFT JOIN region_lists   rl USING (provider_id)
LEFT JOIN postcode_lists pl USING (provider_id)
LEFT JOIN la_lists       ll USING (provider_id);
