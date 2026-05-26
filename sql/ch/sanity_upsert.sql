-- Sanity check: upsert deltas (row count + inactive churn).
--
-- Only safe to run when BOTH companies and companies_staging exist.
-- The Python orchestrator skips this on the bootstrap path.
--
-- Returns one row with:
--   * old_total / new_total / row_delta / row_abs_pct_delta
--     → abort if row_abs_pct_delta > 5% (force=True overrides)
--   * currently_active / would_be_inactivated / inactive_pct
--     → abort if inactive_pct > 5% (force=True overrides)
--
-- Run standalone (requires both tables to exist):
--   $ duckdb ch_bulk.duckdb
--   D .read sql/ch/sanity_upsert.sql

SELECT
    -- Row-count delta
    (SELECT COUNT(*) FROM companies)                              AS old_total,
    (SELECT COUNT(*) FROM companies_staging)                      AS new_total,
    (SELECT COUNT(*) FROM companies_staging) - (SELECT COUNT(*) FROM companies)
                                                                  AS row_delta,
    CASE
        WHEN (SELECT COUNT(*) FROM companies) = 0 THEN NULL
        ELSE ROUND(
            100.0 * abs(
                (SELECT COUNT(*) FROM companies_staging) - (SELECT COUNT(*) FROM companies)
            ) / (SELECT COUNT(*) FROM companies),
            2
        )
    END                                                           AS row_abs_pct_delta,

    -- Inactive churn (only over currently-active rows)
    (SELECT COUNT(*) FROM companies WHERE is_active = TRUE)       AS currently_active,
    (SELECT COUNT(*) FROM companies c
        WHERE c.is_active = TRUE
          AND NOT EXISTS (
              SELECT 1 FROM companies_staging s
              WHERE s.company_number = c.company_number
          ))                                                      AS would_be_inactivated,
    CASE
        WHEN (SELECT COUNT(*) FROM companies WHERE is_active = TRUE) = 0 THEN NULL
        ELSE ROUND(
            100.0 * (SELECT COUNT(*) FROM companies c
                        WHERE c.is_active = TRUE
                          AND NOT EXISTS (
                              SELECT 1 FROM companies_staging s
                              WHERE s.company_number = c.company_number
                          ))
            / (SELECT COUNT(*) FROM companies WHERE is_active = TRUE),
            2
        )
    END                                                           AS inactive_pct;
