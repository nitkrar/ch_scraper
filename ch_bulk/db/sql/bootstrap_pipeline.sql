-- Bootstrap the homecare pipeline tables that extend the CH + CQC base.
--
-- Safe to re-run. Uses IF NOT EXISTS throughout so the Python
-- bootstrapper can call it idempotently.

CREATE SEQUENCE IF NOT EXISTS excluded_staging_row_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS cqc_api_response_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS company_website_id_seq START 1;

CREATE TABLE IF NOT EXISTS exclusion_lists (
    list_name TEXT PRIMARY KEY,
    source TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_imported_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS excluded_companies (
    company_number TEXT NOT NULL,
    list_name TEXT NOT NULL REFERENCES exclusion_lists(list_name),
    reason TEXT,
    excluded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (company_number, list_name)
);

CREATE TABLE IF NOT EXISTS excluded_staging (
    row_id INTEGER PRIMARY KEY DEFAULT nextval('excluded_staging_row_id_seq'),
    raw_name TEXT,
    raw_postcode TEXT,
    raw_reason TEXT,
    resolved_number TEXT,
    match_method TEXT,
    match_score INTEGER,
    needs_review BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS cqc_sync_batches (
    batch_id UUID PRIMARY KEY,
    sync_type TEXT NOT NULL,
    mode TEXT,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status TEXT NOT NULL,
    records_fetched INTEGER,
    records_updated INTEGER,
    error_count INTEGER
);

CREATE TABLE IF NOT EXISTS ch_cqc_matches (
    company_number TEXT NOT NULL,
    cqc_provider_id TEXT NOT NULL,
    total_score INTEGER NOT NULL,
    match_signals JSON NOT NULL,
    status TEXT NOT NULL,
    matched_at TIMESTAMP NOT NULL,
    PRIMARY KEY (company_number, cqc_provider_id)
);

CREATE TABLE IF NOT EXISTS company_enrichment (
    company_number TEXT PRIMARY KEY,
    avg_director_age INTEGER,
    min_director_age INTEGER,
    max_director_age INTEGER,
    directors_over_60 INTEGER,
    all_directors_60_plus BOOLEAN,
    directors_dob_years JSON,
    total_active_directors INTEGER,
    directors JSON,
    revenue DOUBLE,
    revenue_source TEXT,
    employee_count INTEGER,
    filing_period_start DATE,
    filing_period_end DATE,
    gross_profit DOUBLE,
    profit_before_tax DOUBLE,
    profit_after_tax DOUBLE,
    fixed_assets DOUBLE,
    current_assets DOUBLE,
    total_assets DOUBLE,
    net_assets DOUBLE,
    net_current_assets DOUBLE,
    filing_id TEXT,
    filing_format TEXT,
    filing_age_months INTEGER,
    last_enriched_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS cqc_api_responses (
    response_id INTEGER PRIMARY KEY DEFAULT nextval('cqc_api_response_id_seq'),
    batch_id UUID NOT NULL REFERENCES cqc_sync_batches(batch_id),
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    fetched_at TIMESTAMP NOT NULL,
    scrape_date DATE NOT NULL,
    http_status INTEGER,
    raw_json JSON NOT NULL,
    UNIQUE (entity_type, entity_id, fetched_at)
);

CREATE TABLE IF NOT EXISTS cqc_providers_enriched (
    provider_id TEXT PRIMARY KEY,
    companies_house_number TEXT,
    charity_number TEXT,
    ownership_type TEXT,
    brand_id TEXT,
    brand_name TEXT,
    company_name TEXT,
    registration_date DATE,
    deregistration_date DATE,
    registration_status TEXT,
    postal_address_line_1 TEXT,
    postal_address_line_2 TEXT,
    postal_town TEXT,
    postal_county TEXT,
    postcode TEXT,
    region TEXT,
    local_authority TEXT,
    latitude DOUBLE,
    longitude DOUBLE,
    main_phone_number TEXT,
    website TEXT,
    nominated_individual TEXT,
    main_partner TEXT,
    inspection_directorate TEXT,
    current_overall_rating TEXT,
    current_ratings JSON,
    rating_safe TEXT,
    rating_effective TEXT,
    rating_caring TEXT,
    rating_responsive TEXT,
    rating_well_led TEXT,
    regulated_activities JSON,
    relationships JSON,
    number_of_locations INTEGER,
    last_inspection_date DATE,
    last_report_date DATE,
    api_response_id INTEGER REFERENCES cqc_api_responses(response_id),
    enriched_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS cqc_locations_enriched (
    location_id TEXT PRIMARY KEY,
    provider_id TEXT,
    care_home BOOLEAN,
    number_of_beds INTEGER,
    dormancy BOOLEAN,
    registration_date DATE,
    deregistration_date DATE,
    registration_status TEXT,
    postal_address_line_1 TEXT,
    postal_address_line_2 TEXT,
    postal_town TEXT,
    postal_county TEXT,
    postcode TEXT,
    region TEXT,
    local_authority TEXT,
    latitude DOUBLE,
    longitude DOUBLE,
    uprn TEXT,
    paf TEXT,
    main_phone_number TEXT,
    website TEXT,
    registered_manager_name TEXT,
    registered_manager_absent_date DATE,
    inspection_directorate TEXT,
    primary_inspection_category TEXT,
    current_overall_rating TEXT,
    current_ratings JSON,
    rating_safe TEXT,
    rating_effective TEXT,
    rating_caring TEXT,
    rating_responsive TEXT,
    rating_well_led TEXT,
    gac_service_types JSON,
    specialisms JSON,
    regulated_activities JSON,
    relationships JSON,
    last_inspection_date DATE,
    last_report_date DATE,
    api_response_id INTEGER REFERENCES cqc_api_responses(response_id),
    enriched_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS classification_batches (
    batch_id UUID PRIMARY KEY,
    classifier TEXT NOT NULL,
    source_type TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status TEXT NOT NULL,
    input_count INTEGER,
    classified_count INTEGER,
    unable_count INTEGER,
    error_count INTEGER,
    model_version TEXT
);

CREATE TABLE IF NOT EXISTS classifications (
    company_number TEXT NOT NULL,
    source_type TEXT NOT NULL,
    verdict TEXT NOT NULL,
    verdict_reason TEXT,
    evidence_quote TEXT,
    source_url TEXT,
    classifier TEXT NOT NULL,
    classified_at TIMESTAMP NOT NULL,
    batch_id UUID REFERENCES classification_batches(batch_id),
    PRIMARY KEY (company_number, source_type)
);

CREATE TABLE IF NOT EXISTS company_websites (
    website_id INTEGER PRIMARY KEY DEFAULT nextval('company_website_id_seq'),
    company_number TEXT NOT NULL,
    discovered_via TEXT NOT NULL,
    source_entity_id TEXT,
    url TEXT NOT NULL,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    last_seen_reachable_at TIMESTAMP,
    discovered_at TIMESTAMP NOT NULL,
    discovered_by_batch UUID,
    UNIQUE (company_number, url)
);

CREATE INDEX IF NOT EXISTS idx_cqc_api_responses_scrape_date
    ON cqc_api_responses (scrape_date);
CREATE INDEX IF NOT EXISTS idx_ch_cqc_matches_provider_id
    ON ch_cqc_matches (cqc_provider_id);
CREATE INDEX IF NOT EXISTS idx_classifications_batch_id
    ON classifications (batch_id);
CREATE INDEX IF NOT EXISTS idx_company_websites_company_number
    ON company_websites (company_number);

-- DuckDB does not support partial indexes; indexing the CASE expression
-- preserves the same invariant because UNIQUE allows multiple NULLs.
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_primary_per_company
    ON company_websites (
        (CASE WHEN is_primary THEN company_number ELSE NULL END)
    );
