-- Indexes on `companies`
--
-- Idempotent via IF NOT EXISTS. Safe to run after bootstrap, after every
-- upsert, or standalone after manual schema changes.
--
-- The UNIQUE INDEX on company_number is the backstop for the strict-zero
-- duplicate sanity check in sanity_checks.sql — if a duplicate ever slips
-- past that check, this index build fails loudly rather than silently
-- corrupting the table.
--
-- Run standalone:
--   $ duckdb ch_bulk.duckdb
--   D .read sql/ch/indexes.sql

-- Uniqueness guarantee on the natural key. Doubles as the join-acceleration
-- index for upsert_companies.sql (the DELETE ... WHERE IN (...) and the
-- UPDATE ... WHERE NOT IN (...) both lean on it).
CREATE UNIQUE INDEX IF NOT EXISTS idx_company_number_unique ON companies (company_number);

CREATE INDEX IF NOT EXISTS idx_sic1           ON companies (sic_code_1);
CREATE INDEX IF NOT EXISTS idx_sic2           ON companies (sic_code_2);
CREATE INDEX IF NOT EXISTS idx_sic3           ON companies (sic_code_3);
CREATE INDEX IF NOT EXISTS idx_sic4           ON companies (sic_code_4);
CREATE INDEX IF NOT EXISTS idx_company_status ON companies (company_status);
CREATE INDEX IF NOT EXISTS idx_is_active      ON companies (is_active);
CREATE INDEX IF NOT EXISTS idx_sic1_status    ON companies (sic_code_1, company_status);
CREATE INDEX IF NOT EXISTS idx_sic1_active    ON companies (sic_code_1, is_active);
