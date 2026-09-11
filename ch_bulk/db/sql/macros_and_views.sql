-- Macros + views for the homecare pipeline's scoring and prioritisation.
--
-- This file assumes the CH `companies` table already exists. The Python
-- bootstrapper defers execution until that condition is true.

CREATE OR REPLACE MACRO class_score(verdict) AS (
    CASE
        WHEN verdict IS NULL OR trim(verdict) = '' THEN 0
        WHEN lower(trim(verdict)) LIKE 'unable to classify%' THEN 0

        WHEN strpos(lower(trim(verdict)), 'mixed') > 0
             AND (
                strpos(lower(trim(verdict)), 'domiciliary') > 0
                OR strpos(lower(trim(verdict)), 'domicilairy') > 0
             )
             AND strpos(lower(trim(verdict)), 'supported') > 0
        THEN 2

        WHEN strpos(lower(trim(verdict)), 'mixed') > 0
             AND (
                strpos(lower(trim(verdict)), 'domiciliary') > 0
                OR strpos(lower(trim(verdict)), 'domicilairy') > 0
             )
             AND strpos(lower(trim(verdict)), 'residential') > 0
        THEN 1

        WHEN (
            strpos(lower(trim(verdict)), 'majority') > 0
            OR lower(trim(verdict)) LIKE 'likely_%'
            OR lower(trim(verdict)) LIKE 'primarily %'
            OR lower(trim(verdict)) LIKE 'predominantly %'
        )
        THEN CASE
            WHEN strpos(lower(trim(verdict)), 'residential') > 0
                 AND strpos(lower(trim(verdict)), 'domiciliary') = 0
                 AND strpos(lower(trim(verdict)), 'domicilairy') = 0
            THEN 0
            WHEN strpos(lower(trim(verdict)), 'supported') > 0
                 AND strpos(lower(trim(verdict)), 'residential') = 0
                 AND strpos(lower(trim(verdict)), 'domiciliary') = 0
                 AND strpos(lower(trim(verdict)), 'domicilairy') = 0
            THEN 1
            WHEN strpos(lower(trim(verdict)), 'domiciliary') > 0
                 OR strpos(lower(trim(verdict)), 'domicilairy') > 0
            THEN 3
            ELSE 0
        END

        WHEN (
            strpos(lower(trim(verdict)), 'domiciliary') > 0
            OR strpos(lower(trim(verdict)), 'domicilairy') > 0
        )
        AND strpos(lower(trim(verdict)), 'residential') = 0
        AND strpos(lower(trim(verdict)), 'supported') = 0
        THEN 3

        WHEN strpos(lower(trim(verdict)), 'supported') > 0
             AND strpos(lower(trim(verdict)), 'domiciliary') = 0
             AND strpos(lower(trim(verdict)), 'domicilairy') = 0
             AND strpos(lower(trim(verdict)), 'residential') = 0
        THEN 1

        WHEN strpos(lower(trim(verdict)), 'residential') > 0
             AND strpos(lower(trim(verdict)), 'domiciliary') = 0
             AND strpos(lower(trim(verdict)), 'domicilairy') = 0
             AND strpos(lower(trim(verdict)), 'supported') = 0
        THEN 0

        ELSE 0
    END
);

CREATE OR REPLACE MACRO age_score(avg_age, over_60, all_60_plus) AS (
    CASE
        WHEN all_60_plus IS TRUE THEN 3
        WHEN over_60 IS NOT NULL AND over_60 >= 1
             AND avg_age IS NOT NULL AND avg_age >= 55 THEN 2
        WHEN avg_age IS NOT NULL AND avg_age >= 50 THEN 1
        ELSE 0
    END
);

CREATE OR REPLACE MACRO size_score(revenue) AS (
    CASE
        WHEN revenue IS NULL THEN 0
        WHEN revenue BETWEEN 5000000 AND 25000000 THEN 2
        WHEN revenue BETWEEN 1000000 AND 4999999.999999 THEN 1
        WHEN revenue > 25000000 AND revenue <= 50000000 THEN 1
        ELSE 0
    END
);

CREATE OR REPLACE MACRO cqc_class_score(has_homecare, has_supported_living, has_residential) AS (
    CASE
        WHEN has_homecare AND NOT has_supported_living AND NOT has_residential THEN 3
        WHEN has_homecare AND has_supported_living AND NOT has_residential THEN 2
        WHEN has_homecare AND has_residential THEN 1
        WHEN NOT has_homecare AND has_supported_living AND NOT has_residential THEN 1
        ELSE 0
    END
);

CREATE OR REPLACE MACRO tier_for(total, class_pts) AS (
    CASE
        WHEN class_pts = 0 OR total = 0 THEN 'Excluded'
        WHEN total >= 6 THEN 'Tier 1'
        WHEN total >= 4 THEN 'Tier 2'
        ELSE 'Tier 3'
    END
);

CREATE OR REPLACE VIEW current_company_match AS
WITH hsca_active_location_counts AS (
    SELECT
        provider_id,
        COUNT(*) AS active_location_count
    FROM cqc_hsca_locations
    GROUP BY provider_id
),
ranked AS (
    SELECT
        m.company_number,
        m.cqc_provider_id,
        m.total_score,
        m.match_signals,
        m.status,
        m.matched_at,
        ROW_NUMBER() OVER (
            PARTITION BY m.company_number
            ORDER BY
                CASE m.status
                    WHEN 'user_confirmed' THEN 1
                    WHEN 'auto_confirmed' THEN 2
                    WHEN 'needs_review' THEN 3
                    ELSE 99
                END,
                m.total_score DESC,
                m.matched_at DESC,
                COALESCE(h.active_location_count, 0) DESC,
                m.cqc_provider_id ASC
        ) AS rn
    FROM ch_cqc_matches m
    LEFT JOIN hsca_active_location_counts h
        ON h.provider_id = m.cqc_provider_id
    WHERE m.status IN ('user_confirmed', 'auto_confirmed', 'needs_review')
)
SELECT
    company_number,
    cqc_provider_id,
    total_score,
    match_signals,
    status,
    matched_at
FROM ranked
WHERE rn = 1;

CREATE OR REPLACE VIEW company_current_classification AS
WITH ranked AS (
    SELECT
        company_number,
        verdict,
        source_type,
        evidence_quote,
        source_url,
        classified_at,
        classifier,
        ROW_NUMBER() OVER (
            PARTITION BY company_number
            ORDER BY
                CASE source_type
                    WHEN 'manual' THEN 1
                    WHEN 'website' THEN 2
                    WHEN 'cqc_service_type' THEN 3
                    WHEN 'cqc_specialism' THEN 4
                    WHEN 'rule' THEN 5
                    ELSE 99
                END,
                classified_at DESC
        ) AS priority,
        COUNT(DISTINCT verdict) OVER (
            PARTITION BY company_number
        ) AS distinct_verdicts,
        COUNT(*) OVER (
            PARTITION BY company_number
        ) AS source_count
    FROM classifications
)
SELECT
    company_number,
    verdict,
    source_type,
    evidence_quote,
    source_url,
    classified_at,
    classifier,
    distinct_verdicts,
    source_count,
    CASE
        WHEN source_count >= 2 AND distinct_verdicts = 1 THEN 'agree'
        WHEN source_count >= 2 AND distinct_verdicts > 1 THEN 'disagree'
        ELSE 'single_source'
    END AS sources_agreement
FROM ranked
WHERE priority = 1;

CREATE OR REPLACE VIEW tiered_targets AS
WITH cqc_service_mix AS (
    SELECT
        m.company_number,
        BOOL_OR(l.service_types LIKE '%Homecare%') AS has_homecare,
        BOOL_OR(l.service_types LIKE '%Supported living%') AS has_supported_living,
        BOOL_OR(
            l.service_types LIKE '%Residential%'
            OR l.service_types LIKE '%Nursing home%'
        ) AS has_residential
    FROM current_company_match m
    JOIN cqc_locations l ON l.provider_id = m.cqc_provider_id
    GROUP BY m.company_number
),
base AS (
    SELECT
        c.company_number,
        c.company_name,
        c.postcode,
        c.address_post_town,
        ce.avg_director_age,
        ce.directors_over_60,
        ce.all_directors_60_plus,
        ce.revenue,
        ce.revenue_source,
        ce.employee_count,
        ce.total_active_directors,
        cls.verdict,
        cls.source_type AS classification_source,
        cls.evidence_quote,
        cls.source_url,
        cls.sources_agreement,
        m.cqc_provider_id,
        m.status AS match_status,
        m.total_score AS match_score,
        class_score(cls.verdict) AS class_pts,
        cqc_class_score(
            COALESCE(csm.has_homecare, FALSE),
            COALESCE(csm.has_supported_living, FALSE),
            COALESCE(csm.has_residential, FALSE)
        ) AS cqc_class_pts,
        age_score(
            ce.avg_director_age,
            ce.directors_over_60,
            ce.all_directors_60_plus
        ) AS age_pts,
        size_score(ce.revenue) AS size_pts
    FROM companies c
    INNER JOIN current_company_match m USING (company_number)
    LEFT JOIN company_enrichment ce USING (company_number)
    LEFT JOIN company_current_classification cls USING (company_number)
    LEFT JOIN cqc_service_mix csm USING (company_number)
    WHERE c.company_number NOT IN (
        SELECT ec.company_number
        FROM excluded_companies ec
        JOIN exclusion_lists el USING (list_name)
        WHERE el.is_active
    )
),
scored AS (
    SELECT
        *,
        GREATEST(class_pts, cqc_class_pts) AS effective_class_pts,
        GREATEST(class_pts, cqc_class_pts) + age_pts + size_pts AS total_score
    FROM base
)
SELECT
    company_number,
    company_name,
    postcode,
    address_post_town,
    avg_director_age,
    directors_over_60,
    all_directors_60_plus,
    revenue,
    revenue_source,
    employee_count,
    total_active_directors,
    verdict,
    classification_source,
    evidence_quote,
    source_url,
    sources_agreement,
    cqc_provider_id,
    match_status,
    match_score,
    class_pts,
    cqc_class_pts,
    effective_class_pts,
    age_pts,
    size_pts,
    total_score,
    tier_for(total_score, effective_class_pts) AS tier
FROM scored;
