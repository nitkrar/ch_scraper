-- Bootstrap the dual-registration HSCA sheet.
--
-- This table mirrors the small companion worksheet in the monthly ODS
-- file. Unlike the main HSCA locations table, there is no raw JSON
-- split here because the sheet is already narrow and stable.
--
-- Intentionally no FK to cqc_hsca_locations(location_id): DuckDB's
-- COPY FROM DATABASE compaction path does not preserve parent/child
-- ordering strongly enough for this snapshot-style table pair, so a
-- decorative FK would permanently break compaction for HSCA users.
-- Snapshot refreshes still keep the relationship valid at the
-- application level.

CREATE TABLE IF NOT EXISTS cqc_hsca_dual_registrations (
    location_id TEXT NOT NULL,
    location_name TEXT,
    location_hsca_start_date DATE,
    location_type_sector TEXT,
    provider_id TEXT NOT NULL,
    provider_name TEXT,
    linked_organisation_id TEXT NOT NULL,
    linked_organisation_name TEXT,
    relationship TEXT,
    relationship_start_date DATE,
    primary_id BOOLEAN,
    PRIMARY KEY (location_id, provider_id, linked_organisation_id)
);

CREATE INDEX IF NOT EXISTS idx_cqc_hsca_dual_linked_org
    ON cqc_hsca_dual_registrations (linked_organisation_id);
