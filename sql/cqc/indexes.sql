-- Indexes on cqc_locations and cqc_providers
--
-- Idempotent via IF NOT EXISTS.

CREATE UNIQUE INDEX IF NOT EXISTS idx_cqc_location_id_unique
    ON cqc_locations (location_id);
CREATE INDEX IF NOT EXISTS idx_cqc_location_provider
    ON cqc_locations (provider_id);
CREATE INDEX IF NOT EXISTS idx_cqc_location_postcode
    ON cqc_locations (postcode);
CREATE INDEX IF NOT EXISTS idx_cqc_location_is_active
    ON cqc_locations (is_active);

CREATE UNIQUE INDEX IF NOT EXISTS idx_cqc_provider_id_unique
    ON cqc_providers (provider_id);
CREATE INDEX IF NOT EXISTS idx_cqc_provider_is_active
    ON cqc_providers (is_active);
