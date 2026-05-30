"""CQC API enrichment pipeline for providers and locations."""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import timedelta
from pathlib import Path
from typing import Literal

import duckdb

from ch_bulk.core.cancellation import OperationCancelled, raise_if_cancelled
from ch_bulk.core.logging import FsyncLineLogger
from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.core.paths import DEFAULT_DATA_DIR, default_db_path
from ch_bulk.cqc.api_client import APIResult, CQCAPIClient
from ch_bulk.db.staging import (
    LoadedBatch,
    RAW_API_RESPONSE_INSERT_SQL,
    STAGED_JSONL_SCAN_SQL,
    StagedAPIResponse,
    StagingWriter,
    batch_id_from_staging_path,
    isoformat_utc,
    mark_staging_file_loaded,
    pending_staging_files,
    scan_staged_api_responses,
    summarize_loaded_batches,
    sync_batch_progress,
    with_duckdb_connection,
)
from ch_bulk.db.sync_batches import (
    finish_sync_batch,
    insert_sync_batch,
    update_sync_batch_progress,
    utcnow_naive,
)

logger = logging.getLogger(__name__)

Mode = Literal["incremental", "all", "list"]
INCREMENTAL_LOOKBACK_DAYS = 30
DEFAULT_BATCH_SIZE = 1000
KEY_QUESTION_RATING_FIELDS = [
    ("rating_safe", "safe"),
    ("rating_effective", "effective"),
    ("rating_caring", "caring"),
    ("rating_responsive", "responsive"),
    ("rating_well_led", "well-led"),
]


def _key_question_rating_sql(
    json_expr: str,
    json_path: str,
    rating_name: str,
) -> str:
    return f"""(
        SELECT json_extract_string(kq.value, '$.rating')
        FROM json_each({json_expr}, '{json_path}') AS kq
        WHERE lower(json_extract_string(kq.value, '$.name')) = '{rating_name}'
        ORDER BY TRY_CAST(kq.key AS INTEGER)
        LIMIT 1
    )"""


def _extract_payload_key_question_rating(
    current_ratings: object,
    rating_name: str,
) -> str | None:
    ratings = current_ratings if isinstance(current_ratings, dict) else {}
    overall = ratings.get("overall") if isinstance(ratings, dict) else None
    if not isinstance(overall, dict):
        return None
    key_question_ratings = overall.get("keyQuestionRatings")
    if not isinstance(key_question_ratings, list):
        return None
    for entry in key_question_ratings:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or "").strip().lower()
        if name == rating_name:
            rating = entry.get("rating")
            if rating is None:
                return None
            return str(rating)
    return None

PROVIDER_COLUMNS = [
    "provider_id",
    "companies_house_number",
    "charity_number",
    "ownership_type",
    "brand_id",
    "brand_name",
    "company_name",
    "registration_date",
    "deregistration_date",
    "registration_status",
    "postal_address_line_1",
    "postal_address_line_2",
    "postal_town",
    "postal_county",
    "postcode",
    "region",
    "local_authority",
    "latitude",
    "longitude",
    "main_phone_number",
    "website",
    "nominated_individual",
    "main_partner",
    "inspection_directorate",
    "current_overall_rating",
    "current_ratings",
    "rating_safe",
    "rating_effective",
    "rating_caring",
    "rating_responsive",
    "rating_well_led",
    "regulated_activities",
    "relationships",
    "number_of_locations",
    "last_inspection_date",
    "last_report_date",
    "api_response_id",
    "enriched_at",
]

LOCATION_COLUMNS = [
    "location_id",
    "provider_id",
    "care_home",
    "number_of_beds",
    "dormancy",
    "registration_date",
    "deregistration_date",
    "registration_status",
    "postal_address_line_1",
    "postal_address_line_2",
    "postal_town",
    "postal_county",
    "postcode",
    "region",
    "local_authority",
    "latitude",
    "longitude",
    "uprn",
    "paf",
    "main_phone_number",
    "website",
    "registered_manager_name",
    "registered_manager_absent_date",
    "inspection_directorate",
    "primary_inspection_category",
    "current_overall_rating",
    "current_ratings",
    "rating_safe",
    "rating_effective",
    "rating_caring",
    "rating_responsive",
    "rating_well_led",
    "gac_service_types",
    "specialisms",
    "regulated_activities",
    "relationships",
    "last_inspection_date",
    "last_report_date",
    "api_response_id",
    "enriched_at",
]

PROVIDER_ENRICH_INSERT_SQL = f"""
WITH staged AS (
    SELECT
        entity_type,
        entity_id,
        fetched_at,
        CAST(http_status AS INTEGER) AS http_status,
        raw_json
    FROM {STAGED_JSONL_SCAN_SQL}
),
stored AS (
    SELECT response_id, entity_type, entity_id, fetched_at
    FROM cqc_api_responses
    WHERE batch_id = CAST(? AS UUID)
)
INSERT OR REPLACE INTO cqc_providers_enriched ({", ".join(PROVIDER_COLUMNS)})
SELECT
    s.entity_id AS provider_id,
    json_extract_string(s.raw_json, '$.companiesHouseNumber') AS companies_house_number,
    json_extract_string(s.raw_json, '$.charityNumber') AS charity_number,
    json_extract_string(s.raw_json, '$.ownershipType') AS ownership_type,
    json_extract_string(s.raw_json, '$.brandId') AS brand_id,
    json_extract_string(s.raw_json, '$.brandName') AS brand_name,
    json_extract_string(s.raw_json, '$.name') AS company_name,
    CAST(json_extract_string(s.raw_json, '$.registrationDate') AS DATE) AS registration_date,
    CAST(json_extract_string(s.raw_json, '$.deregistrationDate') AS DATE) AS deregistration_date,
    json_extract_string(s.raw_json, '$.registrationStatus') AS registration_status,
    json_extract_string(s.raw_json, '$.postalAddressLine1') AS postal_address_line_1,
    json_extract_string(s.raw_json, '$.postalAddressLine2') AS postal_address_line_2,
    json_extract_string(s.raw_json, '$.postalAddressTownCity') AS postal_town,
    json_extract_string(s.raw_json, '$.postalAddressCounty') AS postal_county,
    json_extract_string(s.raw_json, '$.postalCode') AS postcode,
    json_extract_string(s.raw_json, '$.region') AS region,
    json_extract_string(s.raw_json, '$.localAuthority') AS local_authority,
    TRY_CAST(json_extract_string(s.raw_json, '$.onspdLatitude') AS DOUBLE) AS latitude,
    TRY_CAST(json_extract_string(s.raw_json, '$.onspdLongitude') AS DOUBLE) AS longitude,
    json_extract_string(s.raw_json, '$.mainPhoneNumber') AS main_phone_number,
    json_extract_string(s.raw_json, '$.website') AS website,
    ni.nominated_individual,
    mp.main_partner,
    json_extract_string(s.raw_json, '$.inspectionDirectorate') AS inspection_directorate,
    json_extract_string(s.raw_json, '$.currentRatings.overall.rating') AS current_overall_rating,
    json_extract(s.raw_json, '$.currentRatings') AS current_ratings,
    {_key_question_rating_sql("s.raw_json", "$.currentRatings.overall.keyQuestionRatings", "safe")} AS rating_safe,
    {_key_question_rating_sql("s.raw_json", "$.currentRatings.overall.keyQuestionRatings", "effective")} AS rating_effective,
    {_key_question_rating_sql("s.raw_json", "$.currentRatings.overall.keyQuestionRatings", "caring")} AS rating_caring,
    {_key_question_rating_sql("s.raw_json", "$.currentRatings.overall.keyQuestionRatings", "responsive")} AS rating_responsive,
    {_key_question_rating_sql("s.raw_json", "$.currentRatings.overall.keyQuestionRatings", "well-led")} AS rating_well_led,
    COALESCE(json_extract(s.raw_json, '$.regulatedActivities'), CAST('[]' AS JSON)) AS regulated_activities,
    COALESCE(json_extract(s.raw_json, '$.relationships'), CAST('[]' AS JSON)) AS relationships,
    COALESCE(json_array_length(json_extract(s.raw_json, '$.locationIds')), 0) AS number_of_locations,
    CAST(json_extract_string(s.raw_json, '$.lastInspection.date') AS DATE) AS last_inspection_date,
    CAST(json_extract_string(s.raw_json, '$.lastReport.publicationDate') AS DATE) AS last_report_date,
    stored.response_id AS api_response_id,
    s.fetched_at AS enriched_at
FROM staged s
JOIN stored
  ON stored.entity_type = s.entity_type
 AND stored.entity_id = s.entity_id
 AND stored.fetched_at = s.fetched_at
LEFT JOIN LATERAL (
    SELECT NULLIF(
        TRIM(
            CONCAT_WS(
                ' ',
                json_extract_string(ra.value, '$.nominatedIndividual.personTitle'),
                json_extract_string(ra.value, '$.nominatedIndividual.personGivenName'),
                json_extract_string(ra.value, '$.nominatedIndividual.personFamilyName')
            )
        ),
        ''
    ) AS nominated_individual
    FROM json_each(s.raw_json, '$.regulatedActivities') AS ra
    WHERE json_extract(ra.value, '$.nominatedIndividual') IS NOT NULL
    ORDER BY TRY_CAST(ra.key AS INTEGER)
    LIMIT 1
) ni ON TRUE
LEFT JOIN LATERAL (
    SELECT COALESCE(
        NULLIF(
            TRIM(
                CONCAT_WS(
                    ' ',
                    json_extract_string(contact.value, '$.personTitle'),
                    json_extract_string(contact.value, '$.personGivenName'),
                    json_extract_string(contact.value, '$.personFamilyName')
                )
            ),
            ''
        ),
        NULLIF(
            TRIM(
                CONCAT_WS(
                    ' ',
                    json_extract_string(s.raw_json, '$.mainPartner.personTitle'),
                    json_extract_string(s.raw_json, '$.mainPartner.personGivenName'),
                    json_extract_string(s.raw_json, '$.mainPartner.personFamilyName')
                )
            ),
            ''
        ),
        json_extract_string(s.raw_json, '$.mainPartner')
    ) AS main_partner
    FROM json_each(s.raw_json, '$.contacts') AS contact
    WHERE EXISTS (
        SELECT 1
        FROM json_each(contact.value, '$.personRoles') AS role
        WHERE lower(json_extract_string(role.value, '$')) = 'main partner'
    )
    ORDER BY TRY_CAST(contact.key AS INTEGER)
    LIMIT 1
) mp ON TRUE
WHERE s.entity_type = 'provider'
  AND s.http_status = 200
"""

LOCATION_ENRICH_INSERT_SQL = f"""
WITH staged AS (
    SELECT
        entity_type,
        entity_id,
        fetched_at,
        CAST(http_status AS INTEGER) AS http_status,
        raw_json
    FROM {STAGED_JSONL_SCAN_SQL}
),
stored AS (
    SELECT response_id, entity_type, entity_id, fetched_at
    FROM cqc_api_responses
    WHERE batch_id = CAST(? AS UUID)
)
INSERT OR REPLACE INTO cqc_locations_enriched ({", ".join(LOCATION_COLUMNS)})
SELECT
    s.entity_id AS location_id,
    json_extract_string(s.raw_json, '$.providerId') AS provider_id,
    CASE
        WHEN upper(trim(coalesce(json_extract_string(s.raw_json, '$.careHome'), ''))) IN ('Y', 'YES', 'TRUE', '1') THEN TRUE
        WHEN upper(trim(coalesce(json_extract_string(s.raw_json, '$.careHome'), ''))) IN ('N', 'NO', 'FALSE', '0') THEN FALSE
        ELSE NULL
    END AS care_home,
    TRY_CAST(json_extract_string(s.raw_json, '$.numberOfBeds') AS INTEGER) AS number_of_beds,
    CASE
        WHEN upper(trim(coalesce(json_extract_string(s.raw_json, '$.dormancy'), ''))) IN ('Y', 'YES', 'TRUE', '1') THEN TRUE
        WHEN upper(trim(coalesce(json_extract_string(s.raw_json, '$.dormancy'), ''))) IN ('N', 'NO', 'FALSE', '0') THEN FALSE
        ELSE NULL
    END AS dormancy,
    CAST(json_extract_string(s.raw_json, '$.registrationDate') AS DATE) AS registration_date,
    CAST(json_extract_string(s.raw_json, '$.deregistrationDate') AS DATE) AS deregistration_date,
    json_extract_string(s.raw_json, '$.registrationStatus') AS registration_status,
    json_extract_string(s.raw_json, '$.postalAddressLine1') AS postal_address_line_1,
    json_extract_string(s.raw_json, '$.postalAddressLine2') AS postal_address_line_2,
    json_extract_string(s.raw_json, '$.postalAddressTownCity') AS postal_town,
    json_extract_string(s.raw_json, '$.postalAddressCounty') AS postal_county,
    json_extract_string(s.raw_json, '$.postalCode') AS postcode,
    json_extract_string(s.raw_json, '$.region') AS region,
    json_extract_string(s.raw_json, '$.localAuthority') AS local_authority,
    TRY_CAST(json_extract_string(s.raw_json, '$.onspdLatitude') AS DOUBLE) AS latitude,
    TRY_CAST(json_extract_string(s.raw_json, '$.onspdLongitude') AS DOUBLE) AS longitude,
    json_extract_string(s.raw_json, '$.uprn') AS uprn,
    json_extract_string(s.raw_json, '$.paf') AS paf,
    json_extract_string(s.raw_json, '$.mainPhoneNumber') AS main_phone_number,
    json_extract_string(s.raw_json, '$.website') AS website,
    COALESCE(
        json_extract_string(s.raw_json, '$.registeredManagerName'),
        rm.registered_manager_name
    ) AS registered_manager_name,
    CAST(json_extract_string(s.raw_json, '$.registeredManagerAbsentDate') AS DATE) AS registered_manager_absent_date,
    json_extract_string(s.raw_json, '$.inspectionDirectorate') AS inspection_directorate,
    COALESCE(
        (
            SELECT json_extract_string(cat.value, '$.name')
            FROM json_each(s.raw_json, '$.inspectionCategories') AS cat
            WHERE lower(coalesce(json_extract_string(cat.value, '$.primary'), '')) = 'true'
            ORDER BY TRY_CAST(cat.key AS INTEGER)
            LIMIT 1
        ),
        (
            SELECT json_extract_string(cat.value, '$.name')
            FROM json_each(s.raw_json, '$.inspectionCategories') AS cat
            ORDER BY TRY_CAST(cat.key AS INTEGER)
            LIMIT 1
        )
    ) AS primary_inspection_category,
    json_extract_string(s.raw_json, '$.currentRatings.overall.rating') AS current_overall_rating,
    json_extract(s.raw_json, '$.currentRatings') AS current_ratings,
    {_key_question_rating_sql("s.raw_json", "$.currentRatings.overall.keyQuestionRatings", "safe")} AS rating_safe,
    {_key_question_rating_sql("s.raw_json", "$.currentRatings.overall.keyQuestionRatings", "effective")} AS rating_effective,
    {_key_question_rating_sql("s.raw_json", "$.currentRatings.overall.keyQuestionRatings", "caring")} AS rating_caring,
    {_key_question_rating_sql("s.raw_json", "$.currentRatings.overall.keyQuestionRatings", "responsive")} AS rating_responsive,
    {_key_question_rating_sql("s.raw_json", "$.currentRatings.overall.keyQuestionRatings", "well-led")} AS rating_well_led,
    COALESCE(json_extract(s.raw_json, '$.gacServiceTypes'), CAST('[]' AS JSON)) AS gac_service_types,
    COALESCE(json_extract(s.raw_json, '$.specialisms'), CAST('[]' AS JSON)) AS specialisms,
    COALESCE(json_extract(s.raw_json, '$.regulatedActivities'), CAST('[]' AS JSON)) AS regulated_activities,
    COALESCE(json_extract(s.raw_json, '$.relationships'), CAST('[]' AS JSON)) AS relationships,
    CAST(json_extract_string(s.raw_json, '$.lastInspection.date') AS DATE) AS last_inspection_date,
    CAST(json_extract_string(s.raw_json, '$.lastReport.publicationDate') AS DATE) AS last_report_date,
    stored.response_id AS api_response_id,
    s.fetched_at AS enriched_at
FROM staged s
JOIN stored
  ON stored.entity_type = s.entity_type
 AND stored.entity_id = s.entity_id
 AND stored.fetched_at = s.fetched_at
LEFT JOIN LATERAL (
    SELECT NULLIF(
        TRIM(
            CONCAT_WS(
                ' ',
                json_extract_string(contact.value, '$.personTitle'),
                json_extract_string(contact.value, '$.personGivenName'),
                json_extract_string(contact.value, '$.personFamilyName')
            )
        ),
        ''
    ) AS registered_manager_name
    FROM json_each(s.raw_json, '$.regulatedActivities') AS activity,
         json_each(activity.value, '$.contacts') AS contact
    WHERE EXISTS (
        SELECT 1
        FROM json_each(contact.value, '$.personRoles') AS role
        WHERE lower(json_extract_string(role.value, '$')) = 'registered manager'
    )
    ORDER BY TRY_CAST(activity.key AS INTEGER), TRY_CAST(contact.key AS INTEGER)
    LIMIT 1
) rm ON TRUE
WHERE s.entity_type = 'location'
  AND s.http_status = 200
"""


def _parse_date(value: object) -> object:
    if not value:
        return None
    return value


def _parse_bool_yn(value: object) -> bool | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    if text in {"Y", "YES", "TRUE", "1"}:
        return True
    if text in {"N", "NO", "FALSE", "0"}:
        return False
    return None


def _json_text(value: object) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def _join_name(parts: list[str | None]) -> str | None:
    cleaned = [part.strip() for part in parts if part and str(part).strip()]
    if not cleaned:
        return None
    return " ".join(cleaned)


def _extract_nominated_individual(payload: dict) -> str | None:
    for activity in payload.get("regulatedActivities", []):
        person = activity.get("nominatedIndividual") or {}
        name = _join_name(
            [
                person.get("personTitle"),
                person.get("personGivenName"),
                person.get("personFamilyName"),
            ]
        )
        if name:
            return name
    return None


def _extract_main_partner(payload: dict) -> str | None:
    for contact in payload.get("contacts", []):
        roles = [str(role).lower() for role in contact.get("personRoles", [])]
        if "main partner" in roles:
            return _join_name(
                [
                    contact.get("personTitle"),
                    contact.get("personGivenName"),
                    contact.get("personFamilyName"),
                ]
            )
    person = payload.get("mainPartner") or {}
    if isinstance(person, dict):
        return _join_name(
            [
                person.get("personTitle"),
                person.get("personGivenName"),
                person.get("personFamilyName"),
            ]
        )
    if person:
        return str(person)
    return None


def _extract_registered_manager_name(payload: dict) -> str | None:
    direct = payload.get("registeredManagerName")
    if direct:
        return str(direct)

    for activity in payload.get("regulatedActivities", []):
        for contact in activity.get("contacts", []):
            roles = [str(role).lower() for role in contact.get("personRoles", [])]
            if "registered manager" in roles:
                return _join_name(
                    [
                        contact.get("personTitle"),
                        contact.get("personGivenName"),
                        contact.get("personFamilyName"),
                    ]
                )
    return None


def _extract_primary_inspection_category(payload: dict) -> str | None:
    categories = payload.get("inspectionCategories", [])
    for category in categories:
        if str(category.get("primary", "")).lower() == "true":
            return category.get("name")
    if categories:
        return categories[0].get("name")
    return None


def parse_provider_payload(
    payload: dict,
    api_response_id: int | None,
) -> dict[str, object]:
    now = utcnow_naive()
    return {
        "provider_id": payload.get("providerId"),
        "companies_house_number": payload.get("companiesHouseNumber"),
        "charity_number": payload.get("charityNumber"),
        "ownership_type": payload.get("ownershipType"),
        "brand_id": payload.get("brandId"),
        "brand_name": payload.get("brandName"),
        "company_name": payload.get("name"),
        "registration_date": _parse_date(payload.get("registrationDate")),
        "deregistration_date": _parse_date(payload.get("deregistrationDate")),
        "registration_status": payload.get("registrationStatus"),
        "postal_address_line_1": payload.get("postalAddressLine1"),
        "postal_address_line_2": payload.get("postalAddressLine2"),
        "postal_town": payload.get("postalAddressTownCity"),
        "postal_county": payload.get("postalAddressCounty"),
        "postcode": payload.get("postalCode"),
        "region": payload.get("region"),
        "local_authority": payload.get("localAuthority"),
        "latitude": payload.get("onspdLatitude"),
        "longitude": payload.get("onspdLongitude"),
        "main_phone_number": payload.get("mainPhoneNumber"),
        "website": payload.get("website"),
        "nominated_individual": _extract_nominated_individual(payload),
        "main_partner": _extract_main_partner(payload),
        "inspection_directorate": payload.get("inspectionDirectorate"),
        "current_overall_rating": (
            (payload.get("currentRatings") or {})
            .get("overall", {})
            .get("rating")
        ),
        "current_ratings": _json_text(payload.get("currentRatings")),
        "rating_safe": _extract_payload_key_question_rating(
            payload.get("currentRatings"),
            "safe",
        ),
        "rating_effective": _extract_payload_key_question_rating(
            payload.get("currentRatings"),
            "effective",
        ),
        "rating_caring": _extract_payload_key_question_rating(
            payload.get("currentRatings"),
            "caring",
        ),
        "rating_responsive": _extract_payload_key_question_rating(
            payload.get("currentRatings"),
            "responsive",
        ),
        "rating_well_led": _extract_payload_key_question_rating(
            payload.get("currentRatings"),
            "well-led",
        ),
        "regulated_activities": _json_text(payload.get("regulatedActivities", [])),
        "relationships": _json_text(payload.get("relationships", [])),
        "number_of_locations": len(payload.get("locationIds", [])),
        "last_inspection_date": _parse_date(
            (payload.get("lastInspection") or {}).get("date")
        ),
        "last_report_date": _parse_date(
            (payload.get("lastReport") or {}).get("publicationDate")
        ),
        "api_response_id": api_response_id,
        "enriched_at": now,
    }


def parse_location_payload(
    payload: dict,
    api_response_id: int | None,
) -> dict[str, object]:
    now = utcnow_naive()
    return {
        "location_id": payload.get("locationId"),
        "provider_id": payload.get("providerId"),
        "care_home": _parse_bool_yn(payload.get("careHome")),
        "number_of_beds": payload.get("numberOfBeds"),
        "dormancy": _parse_bool_yn(payload.get("dormancy")),
        "registration_date": _parse_date(payload.get("registrationDate")),
        "deregistration_date": _parse_date(payload.get("deregistrationDate")),
        "registration_status": payload.get("registrationStatus"),
        "postal_address_line_1": payload.get("postalAddressLine1"),
        "postal_address_line_2": payload.get("postalAddressLine2"),
        "postal_town": payload.get("postalAddressTownCity"),
        "postal_county": payload.get("postalAddressCounty"),
        "postcode": payload.get("postalCode"),
        "region": payload.get("region"),
        "local_authority": payload.get("localAuthority"),
        "latitude": payload.get("onspdLatitude"),
        "longitude": payload.get("onspdLongitude"),
        "uprn": payload.get("uprn"),
        "paf": payload.get("paf"),
        "main_phone_number": payload.get("mainPhoneNumber"),
        "website": payload.get("website"),
        "registered_manager_name": _extract_registered_manager_name(payload),
        "registered_manager_absent_date": _parse_date(
            payload.get("registeredManagerAbsentDate")
        ),
        "inspection_directorate": payload.get("inspectionDirectorate"),
        "primary_inspection_category": _extract_primary_inspection_category(payload),
        "current_overall_rating": (
            (payload.get("currentRatings") or {})
            .get("overall", {})
            .get("rating")
        ),
        "current_ratings": _json_text(payload.get("currentRatings")),
        "rating_safe": _extract_payload_key_question_rating(
            payload.get("currentRatings"),
            "safe",
        ),
        "rating_effective": _extract_payload_key_question_rating(
            payload.get("currentRatings"),
            "effective",
        ),
        "rating_caring": _extract_payload_key_question_rating(
            payload.get("currentRatings"),
            "caring",
        ),
        "rating_responsive": _extract_payload_key_question_rating(
            payload.get("currentRatings"),
            "responsive",
        ),
        "rating_well_led": _extract_payload_key_question_rating(
            payload.get("currentRatings"),
            "well-led",
        ),
        "gac_service_types": _json_text(payload.get("gacServiceTypes", [])),
        "specialisms": _json_text(payload.get("specialisms", [])),
        "regulated_activities": _json_text(payload.get("regulatedActivities", [])),
        "relationships": _json_text(payload.get("relationships", [])),
        "last_inspection_date": _parse_date(
            (payload.get("lastInspection") or {}).get("date")
        ),
        "last_report_date": _parse_date(
            (payload.get("lastReport") or {}).get("publicationDate")
        ),
        "api_response_id": api_response_id,
        "enriched_at": now,
    }


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _cqc_expected_entity_type(sync_type: str) -> Literal["provider", "location"]:
    if sync_type == "api_providers":
        return "provider"
    if sync_type == "api_locations":
        return "location"
    raise ValueError(f"Unsupported CQC sync_type: {sync_type}")


def _cqc_insert_sql(sync_type: str) -> str:
    if sync_type == "api_providers":
        return PROVIDER_ENRICH_INSERT_SQL
    if sync_type == "api_locations":
        return LOCATION_ENRICH_INSERT_SQL
    raise ValueError(f"Unsupported CQC sync_type: {sync_type}")


def _cqc_target_table(sync_type: str) -> str:
    if sync_type == "api_providers":
        return "cqc_providers_enriched"
    if sync_type == "api_locations":
        return "cqc_locations_enriched"
    raise ValueError(f"Unsupported CQC sync_type: {sync_type}")


def _load_cqc_staging_file(
    db_path: str | Path,
    *,
    sync_type: str,
    path: Path,
    final_status: str = "succeeded",
) -> LoadedBatch:
    batch_id = batch_id_from_staging_path(sync_type, path)

    def read_existing(
        con: duckdb.DuckDBPyConnection,
    ) -> tuple[int, int, int]:
        ensure_pipeline_schema(con)
        return sync_batch_progress(con, batch_id)

    (
        existing_records_fetched,
        existing_records_updated,
        existing_error_count,
    ) = with_duckdb_connection(db_path, read_existing)

    staged_stats = scan_staged_api_responses(
        path,
        expected_entity_type=_cqc_expected_entity_type(sync_type),
        success_json_type="OBJECT",
    )
    final_records_fetched = max(
        existing_records_fetched,
        staged_stats.total_rows,
    )
    final_records_updated = existing_records_updated
    final_error_count = existing_error_count

    try:
        if staged_stats.invalid_entity_rows:
            raise ValueError(
                f"Unexpected entity_type rows in {sync_type} staging: {path}"
            )
        if staged_stats.invalid_payload_rows:
            raise ValueError(
                f"Expected object payloads for successful {sync_type} staging rows"
            )

        def run_load(con: duckdb.DuckDBPyConnection) -> None:
            nonlocal final_records_fetched
            nonlocal final_records_updated
            nonlocal final_error_count

            ensure_pipeline_schema(con)
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute(
                    RAW_API_RESPONSE_INSERT_SQL,
                    [batch_id, str(path)],
                )
                con.execute(
                    _cqc_insert_sql(sync_type),
                    [str(path), batch_id],
                )

                raw_counts = con.execute(
                    """
                    SELECT
                        COUNT(*) AS records_fetched,
                        COUNT(*) FILTER (WHERE http_status != 200) AS non_200_errors
                    FROM cqc_api_responses
                    WHERE batch_id = CAST(? AS UUID)
                    """,
                    [batch_id],
                ).fetchone()
                updated_row = con.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {_cqc_target_table(sync_type)} target
                    JOIN cqc_api_responses responses
                      ON responses.response_id = target.api_response_id
                    WHERE responses.batch_id = CAST(? AS UUID)
                    """,
                    [batch_id],
                ).fetchone()
                final_records_fetched = max(
                    existing_records_fetched,
                    int(raw_counts[0]) if raw_counts else 0,
                )
                final_records_updated = max(
                    existing_records_updated,
                    int(updated_row[0]) if updated_row else 0,
                )
                final_error_count = max(
                    existing_error_count,
                    int(raw_counts[1]) if raw_counts else 0,
                )
                finish_sync_batch(
                    con,
                    batch_id,
                    status=final_status,
                    records_fetched=final_records_fetched,
                    records_updated=final_records_updated,
                    error_count=final_error_count,
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

        with_duckdb_connection(db_path, run_load)
    except Exception:
        def mark_failed(con: duckdb.DuckDBPyConnection) -> None:
            finish_sync_batch(
                con,
                batch_id,
                status="failed",
                records_fetched=final_records_fetched,
                records_updated=final_records_updated,
                error_count=final_error_count + 1,
            )

        with_duckdb_connection(db_path, mark_failed)
        raise

    loaded_path = mark_staging_file_loaded(path)
    return LoadedBatch(
        batch_id=batch_id,
        path=str(loaded_path),
        records_fetched=final_records_fetched,
        records_updated=final_records_updated,
        error_count=final_error_count,
    )


def load_cqc_staging(
    data_dir: str | Path,
    db_path: str | Path,
    *,
    sync_type: str,
    batch_id: str | None = None,
    final_status: str = "succeeded",
) -> dict[str, object]:
    loaded: list[LoadedBatch] = []
    for path in pending_staging_files(
        data_dir,
        sync_type=sync_type,
        batch_id=batch_id,
    ):
        loaded.append(
            _load_cqc_staging_file(
                db_path,
                sync_type=sync_type,
                path=path,
                final_status=final_status,
            )
        )
    return summarize_loaded_batches(
        sync_type,
        loaded,
    )


def _backfill_cqc_ratings_table(
    con: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
) -> int:
    row = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    row_count = int(row[0]) if row else 0
    if row_count <= 0:
        return 0
    assignments = ",\n                ".join(
        f"{column_name} = {_key_question_rating_sql('current_ratings', '$.overall.keyQuestionRatings', rating_name)}"
        for column_name, rating_name in KEY_QUESTION_RATING_FIELDS
    )
    con.execute(
        f"""
        UPDATE {table_name}
        SET {assignments}
        """
    )
    return row_count


def backfill_cqc_ratings(db_path: str | Path) -> dict[str, int]:
    def run_backfill(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
        ensure_pipeline_schema(con)
        providers_updated = _backfill_cqc_ratings_table(
            con,
            table_name="cqc_providers_enriched",
        )
        locations_updated = _backfill_cqc_ratings_table(
            con,
            table_name="cqc_locations_enriched",
        )
        return {
            "records_updated": providers_updated + locations_updated,
            "providers_updated": providers_updated,
            "locations_updated": locations_updated,
        }

    return with_duckdb_connection(db_path, run_backfill)


def _validated_mode(mode: str) -> Mode:
    normalized = mode.strip().lower()
    if normalized not in {"incremental", "all", "list"}:
        raise ValueError(f"Unsupported mode: {mode}")
    return normalized  # type: ignore[return-value]


def _validated_batch_size(batch_size: int) -> int:
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    return batch_size


def _progress_text(done: int, total: int) -> str:
    if total <= 0:
        return "0/0 0.0%"
    percent = (done / total) * 100
    return f"{done}/{total} {percent:.1f}%"


def _duration_text(seconds: float) -> str:
    rounded = max(0, int(round(seconds)))
    hours, remainder = divmod(rounded, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


class CQCAPIEnricher:
    def __init__(
        self,
        data_dir: str | Path | None = None,
        db_path: str | Path | None = None,
    ) -> None:
        self.data_dir = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
        self.db_path = Path(db_path) if db_path is not None else default_db_path(self.data_dir)

    def enrich_providers(
        self,
        *,
        mode: str = "incremental",
        ids: list[str] | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, object]:
        return self._enrich(
            entity_type="provider",
            mode=_validated_mode(mode),
            ids=ids,
            batch_size=_validated_batch_size(batch_size),
            cancel_event=cancel_event,
        )

    def enrich_locations(
        self,
        *,
        mode: str = "incremental",
        ids: list[str] | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, object]:
        return self._enrich(
            entity_type="location",
            mode=_validated_mode(mode),
            ids=ids,
            batch_size=_validated_batch_size(batch_size),
            cancel_event=cancel_event,
        )

    def _select_entity_ids(
        self,
        con: duckdb.DuckDBPyConnection,
        *,
        entity_type: Literal["provider", "location"],
        mode: Mode,
        ids: list[str] | None,
    ) -> list[str]:
        if mode == "list":
            if not ids:
                raise ValueError("mode=list requires one or more ids")
            cleaned = [value.strip() for value in ids if value.strip()]
            return _dedupe_preserve_order(cleaned)

        if entity_type == "provider":
            source_table = "cqc_providers"
            id_column = "provider_id"
            enriched_table = "cqc_providers_enriched"
        else:
            source_table = "cqc_locations"
            id_column = "location_id"
            enriched_table = "cqc_locations_enriched"

        if mode == "all":
            rows = con.execute(
                f"""
                SELECT DISTINCT {id_column}
                FROM {source_table}
                WHERE {id_column} IS NOT NULL
                  AND trim({id_column}) != ''
                ORDER BY {id_column}
                """
            ).fetchall()
            return [row[0] for row in rows]

        cutoff = utcnow_naive() - timedelta(days=INCREMENTAL_LOOKBACK_DAYS)
        rows = con.execute(
            f"""
            SELECT DISTINCT s.{id_column}
            FROM {source_table} s
            LEFT JOIN {enriched_table} e
              ON e.{id_column} = s.{id_column}
            WHERE s.{id_column} IS NOT NULL
              AND trim(s.{id_column}) != ''
              AND (
                  e.{id_column} IS NULL
                  OR e.enriched_at IS NULL
                  OR e.enriched_at < ?
              )
            ORDER BY s.{id_column}
            """,
            [cutoff],
        ).fetchall()
        return [row[0] for row in rows]

    def _enrich(
        self,
        *,
        entity_type: Literal["provider", "location"],
        mode: Mode,
        ids: list[str] | None,
        batch_size: int,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, object]:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        batch_id: str | None = None
        run_log: FsyncLineLogger | None = None
        staging_writer: StagingWriter | None = None
        sync_type = "api_providers" if entity_type == "provider" else "api_locations"
        target_ids: list[str] = []
        total_fetched = 0
        total_errors = 0
        durable_fetched = 0
        durable_errors = 0
        staged_since_sync = 0
        processed = 0
        started_monotonic = time.monotonic()
        try:
            load_cqc_staging(
                self.data_dir,
                self.db_path,
                sync_type=sync_type,
            )

            def prepare_batch(
                con: duckdb.DuckDBPyConnection,
            ) -> tuple[list[str], str]:
                ensure_pipeline_schema(con)
                return (
                    self._select_entity_ids(
                        con,
                        entity_type=entity_type,
                        mode=mode,
                        ids=ids,
                    ),
                    insert_sync_batch(
                        con,
                        sync_type=sync_type,
                        mode=mode,
                    ),
                )

            target_ids, batch_id = with_duckdb_connection(
                self.db_path,
                prepare_batch,
            )

            run_log = FsyncLineLogger(
                self.data_dir,
                sync_type=sync_type,
                batch_id=batch_id,
            )
            staging_writer = StagingWriter(
                self.data_dir,
                sync_type=sync_type,
                batch_id=batch_id,
            )
            run_log.write_line(
                f"start sync_type={sync_type} mode={mode} requested={len(target_ids)} batch_size={batch_size}"
            )
            run_log.flush_and_fsync()

            # We only fsync the staging file/log at batch boundaries. A hard
            # kill in the middle of a batch can lose up to batch_size staged
            # responses/log lines, and the sync-batch row can lag by the same
            # amount. Clean exits and handled exceptions fsync the partial
            # batch before marking the batch row.
            def checkpoint_write_phase() -> None:
                nonlocal durable_fetched
                nonlocal durable_errors
                nonlocal staged_since_sync
                if batch_id is None or run_log is None or staging_writer is None:
                    return
                if (
                    staged_since_sync == 0
                    and total_fetched == durable_fetched
                    and total_errors == durable_errors
                ):
                    run_log.flush_and_fsync()
                    return

                staging_writer.flush_and_fsync()

                def update_progress(con: duckdb.DuckDBPyConnection) -> None:
                    update_sync_batch_progress(
                        con,
                        batch_id,
                        records_fetched=total_fetched,
                        records_updated=0,
                        error_count=total_errors,
                    )

                with_duckdb_connection(self.db_path, update_progress)
                elapsed_seconds = time.monotonic() - started_monotonic
                eta_seconds = (
                    (elapsed_seconds / processed) * max(len(target_ids) - processed, 0)
                    if processed
                    else 0.0
                )
                run_log.write_line(
                    "flush "
                    f"batch_size={staged_since_sync} "
                    f"total_fetched={total_fetched} "
                    f"errors={total_errors} "
                    f"elapsed={_duration_text(elapsed_seconds)} "
                    f"eta={_duration_text(eta_seconds)}"
                )
                run_log.flush_and_fsync()
                durable_fetched = total_fetched
                durable_errors = total_errors
                staged_since_sync = 0

            with CQCAPIClient(self.data_dir) as client:
                for entity_id in target_ids:
                    raise_if_cancelled(
                        cancel_event,
                        reason=f"CQC {entity_type} enrichment cancelled",
                    )
                    status = "error"
                    http_status: int | None = None
                    try:
                        response: APIResult
                        if entity_type == "provider":
                            if cancel_event is None:
                                response = client.get_provider(entity_id)
                            else:
                                response = client.get_provider(
                                    entity_id,
                                    cancel_event=cancel_event,
                                )
                        else:
                            if cancel_event is None:
                                response = client.get_location(entity_id)
                            else:
                                response = client.get_location(
                                    entity_id,
                                    cancel_event=cancel_event,
                                )

                        http_status = response.status_code
                        staging_writer.append(
                            StagedAPIResponse(
                                entity_type=entity_type,
                                entity_id=entity_id,
                                fetched_at=isoformat_utc(),
                                http_status=response.status_code,
                                raw_json=response.payload,
                            )
                        )
                        total_fetched += 1
                        staged_since_sync += 1

                        if response.status_code != 200:
                            total_errors += 1
                            status = "skip" if response.status_code == 404 else "error"
                        else:
                            status = "ok"
                    except OperationCancelled:
                        raise
                    except Exception:
                        total_errors += 1
                        logger.exception(
                            "Failed to enrich %s %s",
                            entity_type,
                            entity_id,
                        )
                    finally:
                        processed += 1
                        status_bits = [
                            f"entity_id={entity_id}",
                            f"status={status}",
                        ]
                        if http_status is not None:
                            status_bits.append(f"http_status={http_status}")
                        status_bits.append(
                            f"progress={_progress_text(processed, len(target_ids))}"
                        )
                        run_log.write_line(" ".join(status_bits))

                    if staged_since_sync >= batch_size:
                        checkpoint_write_phase()

            raise_if_cancelled(
                cancel_event,
                reason=f"CQC {entity_type} enrichment cancelled",
            )
            checkpoint_write_phase()
            load_summary = load_cqc_staging(
                self.data_dir,
                self.db_path,
                sync_type=sync_type,
                batch_id=batch_id,
            )
            records_fetched = int(load_summary["records_fetched"])
            records_updated = int(load_summary["records_updated"])
            error_count = int(load_summary["error_count"])
            elapsed_seconds = time.monotonic() - started_monotonic
            run_log.write_line(
                "complete "
                f"requested={len(target_ids)} "
                f"records_fetched={records_fetched} "
                f"records_updated={records_updated} "
                f"errors={error_count} "
                f"elapsed={_duration_text(elapsed_seconds)}"
            )
            run_log.flush_and_fsync()
            logger.info(
                "CQC API %s enrichment complete: requested=%d fetched=%d updated=%d errors=%d batch_id=%s",
                entity_type,
                len(target_ids),
                records_fetched,
                records_updated,
                error_count,
                batch_id,
            )
            return {
                "batch_id": batch_id,
                "requested": len(target_ids),
                "records_fetched": records_fetched,
                "records_updated": records_updated,
                "error_count": error_count,
                "mode": mode,
                "log_path": str(run_log.path),
            }
        except OperationCancelled:
            if staging_writer is not None:
                staging_writer.flush_and_fsync()
            if batch_id is not None:
                if total_fetched > 0:
                    load_summary = load_cqc_staging(
                        self.data_dir,
                        self.db_path,
                        sync_type=sync_type,
                        batch_id=batch_id,
                        final_status="cancelled",
                    )
                    if run_log is not None:
                        elapsed_seconds = time.monotonic() - started_monotonic
                        run_log.write_line(
                            "cancelled "
                            f"requested={len(target_ids)} "
                            f"records_fetched={load_summary['records_fetched']} "
                            f"records_updated={load_summary['records_updated']} "
                            f"errors={load_summary['error_count']} "
                            f"elapsed={_duration_text(elapsed_seconds)}"
                        )
                        run_log.flush_and_fsync()
                else:
                    def mark_cancelled(con: duckdb.DuckDBPyConnection) -> None:
                        finish_sync_batch(
                            con,
                            batch_id,
                            status="cancelled",
                            records_fetched=0,
                            records_updated=0,
                            error_count=0,
                        )

                    with_duckdb_connection(self.db_path, mark_cancelled)
                    if run_log is not None:
                        elapsed_seconds = time.monotonic() - started_monotonic
                        run_log.write_line(
                            "cancelled "
                            "requested=0 records_fetched=0 records_updated=0 errors=0 "
                            f"elapsed={_duration_text(elapsed_seconds)}"
                        )
                        run_log.flush_and_fsync()
            raise
        except BaseException:
            if staging_writer is not None:
                staging_writer.flush_and_fsync()
            if batch_id is not None:
                def mark_failed(con: duckdb.DuckDBPyConnection) -> None:
                    finish_sync_batch(
                        con,
                        batch_id,
                        status="failed",
                        records_fetched=total_fetched,
                        records_updated=0,
                        error_count=total_errors + 1,
                    )

                with_duckdb_connection(self.db_path, mark_failed)
            if run_log is not None:
                elapsed_seconds = time.monotonic() - started_monotonic
                run_log.write_line(
                    "crash "
                    f"records_fetched={total_fetched} "
                    f"records_updated=0 "
                    f"errors={total_errors + 1} "
                    f"elapsed={_duration_text(elapsed_seconds)}"
                )
                run_log.flush_and_fsync()
            raise
        finally:
            if run_log is not None:
                run_log.close()
            if staging_writer is not None:
                staging_writer.close()
