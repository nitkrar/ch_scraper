-- Bootstrap the HSCA locations table used by the homecare pipeline.
--
-- This is a schema-only file: it creates the durable table and the
-- indexes WP4 will read from. The row-level ingest/upsert logic lives
-- in upsert_hsca_locations.sql because it depends on staging tables.

CREATE TABLE IF NOT EXISTS cqc_hsca_locations (
    location_id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    provider_companies_house_number TEXT,
    provider_charity_number TEXT,
    provider_ownership_type TEXT,
    provider_brand_id TEXT,
    provider_brand_name TEXT,
    provider_web_address TEXT,
    location_web_address TEXT,
    care_home BOOLEAN,
    number_of_beds INTEGER,
    dormant BOOLEAN,
    registered_manager_name TEXT,
    st_domiciliary_care_service BOOLEAN,
    st_supported_living_service BOOLEAN,
    st_care_home_with_nursing BOOLEAN,
    st_care_home_without_nursing BOOLEAN,
    st_extra_care_housing_services BOOLEAN,
    st_hospice_services_at_home BOOLEAN,
    service_user_bands JSON,
    regulated_activities JSON,
    bulk_imported_at TIMESTAMP NOT NULL,
    bulk_file_date DATE NOT NULL,
    raw_row JSON NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cqc_hsca_provider_id
    ON cqc_hsca_locations (provider_id);
CREATE INDEX IF NOT EXISTS idx_cqc_hsca_provider_company_number
    ON cqc_hsca_locations (provider_companies_house_number);
CREATE INDEX IF NOT EXISTS idx_cqc_hsca_bulk_file_date
    ON cqc_hsca_locations (bulk_file_date);
