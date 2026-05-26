-- Sanity check: upsert deltas for cqc_locations.

SELECT
    (SELECT COUNT(*) FROM cqc_locations)                              AS old_total,
    (SELECT COUNT(*) FROM cqc_locations_staging)                      AS new_total,
    (SELECT COUNT(*) FROM cqc_locations_staging) - (SELECT COUNT(*) FROM cqc_locations) AS row_delta,
    CASE
        WHEN (SELECT COUNT(*) FROM cqc_locations) = 0 THEN NULL
        ELSE ROUND(
            100.0 * abs(
                (SELECT COUNT(*) FROM cqc_locations_staging) - (SELECT COUNT(*) FROM cqc_locations)
            ) / (SELECT COUNT(*) FROM cqc_locations),
            2
        )
    END                                                               AS row_abs_pct_delta,
    (SELECT COUNT(*) FROM cqc_locations WHERE is_active = TRUE)       AS currently_active,
    (SELECT COUNT(*) FROM cqc_locations c
        WHERE c.is_active = TRUE
          AND NOT EXISTS (
              SELECT 1 FROM cqc_locations_staging s
              WHERE s.location_id = c.location_id
          ))                                                          AS would_be_inactivated,
    CASE
        WHEN (SELECT COUNT(*) FROM cqc_locations WHERE is_active = TRUE) = 0 THEN NULL
        ELSE ROUND(
            100.0 * (SELECT COUNT(*) FROM cqc_locations c
                        WHERE c.is_active = TRUE
                          AND NOT EXISTS (
                              SELECT 1 FROM cqc_locations_staging s
                              WHERE s.location_id = c.location_id
                          ))
            / (SELECT COUNT(*) FROM cqc_locations WHERE is_active = TRUE),
            2
        )
    END                                                               AS inactive_pct;
