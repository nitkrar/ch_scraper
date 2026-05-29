"""CH to CQC provider matching using the simplified Phase 4 contract."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import duckdb
from rapidfuzz import fuzz

from ch_bulk.db.bootstrap import ensure_pipeline_schema, recover_interrupted_compaction
from ch_bulk.db.sync_batches import utcnow_naive

logger = logging.getLogger(__name__)

MATCH_THRESHOLD = 90
CONFLICT_SCORE = 89
NOISE_TOKENS = {"ltd", "limited", "plc", "llp", "the", "uk"}
AGGRESSIVE_NOISE_TOKENS = NOISE_TOKENS | {"care", "services"}
PREVIOUS_NAME_COLUMNS = [f"prev_name_{idx}" for idx in range(1, 11)]
USER_LOCKED_STATUSES = {"user_confirmed", "user_rejected"}
RELEVANT_HSCA_SERVICE_COLUMNS = [
    "st_domiciliary_care_service",
    "st_supported_living_service",
    "st_care_home_with_nursing",
    "st_care_home_without_nursing",
    "st_extra_care_housing_services",
    "st_hospice_services_at_home",
]

Mode = Literal["incremental", "all"]


@dataclass(frozen=True)
class CompanyCandidate:
    company_number: str
    company_name: str
    company_name_normalized: str
    postcode: str | None
    outward_postcode: str | None
    previous_names: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class ProviderCandidate:
    provider_id: str
    provider_name: str
    provider_name_normalized: str
    postcode_prefixes: tuple[str, ...]
    hsca_company_numbers: tuple[str, ...]
    api_company_number: str | None


@dataclass(frozen=True)
class MatchCandidate:
    company_number: str
    cqc_provider_id: str
    total_score: int
    match_signals: tuple[dict[str, object], ...]
    status: str


def normalize_company_number(value: object) -> str | None:
    text = re.sub(r"\s+", "", str(value or "").strip().upper())
    if not text:
        return None
    if text.isdigit():
        return text.zfill(8)
    return text


def normalize_name(name: object, *, aggressive: bool = False) -> str:
    tokens_to_drop = AGGRESSIVE_NOISE_TOKENS if aggressive else NOISE_TOKENS
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", str(name or "").lower())
    tokens = [token for token in cleaned.split() if token and token not in tokens_to_drop]
    return " ".join(tokens).strip()


def normalize_postcode(postcode: object) -> str | None:
    cleaned = re.sub(r"\s+", "", str(postcode or "").upper())
    return cleaned or None


def postcode_outward(postcode: object) -> str | None:
    normalized = normalize_postcode(postcode)
    if normalized is None:
        return None
    if len(normalized) <= 4:
        return normalized
    return normalized[:-3]


def _validated_mode(mode: str) -> Mode:
    normalized = mode.strip().lower()
    if normalized not in {"incremental", "all"}:
        raise ValueError(f"Unsupported match mode: {mode}")
    return normalized  # type: ignore[return-value]


def _json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def _dedupe_preserve_order(values: list[str]) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return tuple(out)


def _normalized_company_number_sql(expression: str) -> str:
    compact = f"regexp_replace(upper(coalesce({expression}, '')), '\\s+', '', 'g')"
    return (
        "CASE "
        f"WHEN {compact} = '' THEN NULL "
        f"WHEN regexp_full_match({compact}, '^[0-9]+$') THEN lpad({compact}, 8, '0') "
        f"ELSE {compact} "
        "END"
    )


def _outward_postcode_sql(expression: str) -> str:
    normalized = f"regexp_replace(upper(coalesce({expression}, '')), '\\s+', '', 'g')"
    return (
        "CASE "
        f"WHEN {normalized} = '' THEN NULL "
        f"WHEN length({normalized}) <= 4 THEN {normalized} "
        f"ELSE substr({normalized}, 1, length({normalized}) - 3) "
        "END"
    )


def _provider_id_filter_sql(
    provider_ids: list[str] | None,
    params: list[object],
    *,
    alias: str | None = None,
) -> str:
    if not provider_ids:
        return ""

    placeholders = ", ".join(["?"] * len(provider_ids))
    qualifier = f"{alias}." if alias else ""
    params.extend(provider_ids)
    return f" AND {qualifier}provider_id IN ({placeholders})"


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _require_match_inputs(con: duckdb.DuckDBPyConnection) -> None:
    missing = [
        table_name
        for table_name in ("companies", "cqc_providers", "cqc_hsca_locations")
        if not _table_exists(con, table_name)
    ]
    if missing:
        raise RuntimeError(
            "Matching requires these tables first: " + ", ".join(sorted(missing))
        )


def _select_companies(
    con: duckdb.DuckDBPyConnection,
    *,
    company_numbers: list[str] | None,
    provider_ids: list[str] | None,
) -> list[CompanyCandidate]:
    params: list[object] = []
    company_filter = ""
    if company_numbers:
        placeholders = ", ".join(["?"] * len(company_numbers))
        company_filter = f" AND c.company_number IN ({placeholders})"
        params.extend(company_numbers)

    provider_filter = _provider_id_filter_sql(provider_ids, params, alias="p")
    select_prev_names = ", ".join(f"c.{name}" for name in PREVIOUS_NAME_COLUMNS)
    hsca_service_predicate = _relevant_hsca_service_sql(alias="h")
    normalized_hsca_company_number = _normalized_company_number_sql(
        "h.provider_companies_house_number"
    )
    normalized_api_company_number = _normalized_company_number_sql(
        "pe.companies_house_number"
    )
    company_outward_sql = _outward_postcode_sql("c.postcode")
    rows = con.execute(
        f"""
        WITH base_companies AS (
            SELECT *
            FROM companies c
            WHERE c.is_active = TRUE
              AND c.company_status = 'Active'
              {company_filter}
        ),
        in_scope_providers AS (
            SELECT p.provider_id, p.postcode_prefixes_list
            FROM cqc_providers p
            WHERE p.is_active = TRUE
              AND EXISTS (
                  SELECT 1
                  FROM cqc_hsca_locations h
                  WHERE h.provider_id = p.provider_id
                    AND ({hsca_service_predicate})
              )
              {provider_filter}
        ),
        direct_company_numbers AS (
            SELECT DISTINCT {normalized_hsca_company_number} AS company_number
            FROM cqc_hsca_locations h
            INNER JOIN in_scope_providers p USING (provider_id)
            WHERE ({hsca_service_predicate})
              AND {normalized_hsca_company_number} IS NOT NULL

            UNION

            SELECT DISTINCT {normalized_api_company_number} AS company_number
            FROM cqc_providers_enriched pe
            INNER JOIN in_scope_providers p USING (provider_id)
            WHERE {normalized_api_company_number} IS NOT NULL
        ),
        in_scope_outward_codes AS (
            SELECT DISTINCT upper(trim(outward_code)) AS outward_code
            FROM in_scope_providers p,
                UNNEST(p.postcode_prefixes_list) AS prefix(outward_code)
            WHERE outward_code IS NOT NULL
              AND trim(outward_code) != ''
        ),
        candidate_company_numbers AS (
            SELECT company_number
            FROM direct_company_numbers

            UNION

            SELECT DISTINCT c.company_number
            FROM base_companies c
            INNER JOIN in_scope_outward_codes oc
                ON {company_outward_sql} = oc.outward_code
        )
        SELECT c.company_number, c.company_name, c.postcode, {select_prev_names}
        FROM base_companies c
        INNER JOIN candidate_company_numbers USING (company_number)
        ORDER BY c.company_number
        """,
        params,
    ).fetchall()

    companies: list[CompanyCandidate] = []
    for row in rows:
        company_number = normalize_company_number(row[0])
        if company_number is None:
            continue
        company_name = str(row[1] or "")
        previous_names: list[tuple[str, str]] = []
        for raw_prev_name in row[3:]:
            raw = str(raw_prev_name or "").strip()
            normalized = normalize_name(raw)
            if raw and normalized:
                previous_names.append((raw, normalized))
        companies.append(
            CompanyCandidate(
                company_number=company_number,
                company_name=company_name,
                company_name_normalized=normalize_name(company_name),
                postcode=str(row[2]).strip() if row[2] else None,
                outward_postcode=postcode_outward(row[2]),
                previous_names=tuple(previous_names),
            )
        )
    return companies


def _relevant_hsca_service_sql(alias: str = "h") -> str:
    return " OR ".join(
        f"COALESCE({alias}.{column_name}, FALSE)"
        for column_name in RELEVANT_HSCA_SERVICE_COLUMNS
    )


def _select_providers(
    con: duckdb.DuckDBPyConnection,
    *,
    provider_ids: list[str] | None,
) -> list[ProviderCandidate]:
    params: list[object] = []
    provider_filter = _provider_id_filter_sql(provider_ids, params, alias="p")

    provider_rows = con.execute(
        f"""
        SELECT
            p.provider_id,
            p.provider_name,
            p.postcode_prefixes_list,
            pe.companies_house_number
        FROM cqc_providers p
        LEFT JOIN cqc_providers_enriched pe USING (provider_id)
        WHERE p.is_active = TRUE
          AND EXISTS (
              SELECT 1
              FROM cqc_hsca_locations h
              WHERE h.provider_id = p.provider_id
                AND ({_relevant_hsca_service_sql()})
          )
          {provider_filter}
        ORDER BY p.provider_id
        """,
        params,
    ).fetchall()

    hsca_filter = ""
    hsca_params: list[object] = []
    if provider_ids:
        hsca_filter = _provider_id_filter_sql(provider_ids, hsca_params)

    hsca_rows = con.execute(
        f"""
        SELECT DISTINCT provider_id, provider_companies_house_number
        FROM cqc_hsca_locations
        WHERE ({_relevant_hsca_service_sql(alias='cqc_hsca_locations')})
          AND provider_companies_house_number IS NOT NULL
          AND trim(provider_companies_house_number) != ''
          {hsca_filter}
        ORDER BY provider_id, provider_companies_house_number
        """,
        hsca_params,
    ).fetchall()

    hsca_numbers_by_provider: dict[str, list[str]] = {}
    for provider_id, company_number in hsca_rows:
        normalized = normalize_company_number(company_number)
        if normalized is None:
            continue
        hsca_numbers_by_provider.setdefault(str(provider_id), []).append(normalized)

    providers: list[ProviderCandidate] = []
    for provider_id, provider_name, postcode_prefixes_list, api_company_number in provider_rows:
        prefixes = [
            prefix
            for prefix in (
                normalize_postcode(value)
                for value in (postcode_prefixes_list or [])
            )
            if prefix
        ]
        api_number = normalize_company_number(api_company_number)
        providers.append(
            ProviderCandidate(
                provider_id=str(provider_id),
                provider_name=str(provider_name or ""),
                provider_name_normalized=normalize_name(provider_name),
                postcode_prefixes=_dedupe_preserve_order(prefixes),
                hsca_company_numbers=_dedupe_preserve_order(
                    hsca_numbers_by_provider.get(str(provider_id), [])
                ),
                api_company_number=api_number,
            )
        )
    return providers


def _token_set_ratio(left: str, right: str) -> int:
    if not left or not right:
        return 0
    return int(round(fuzz.token_set_ratio(left, right)))


def _provider_source_numbers(
    provider: ProviderCandidate,
) -> dict[str, tuple[str, ...]]:
    values: dict[str, tuple[str, ...]] = {}
    if provider.hsca_company_numbers:
        values["hsca"] = provider.hsca_company_numbers
    if provider.api_company_number:
        values["api"] = (provider.api_company_number,)
    return values


def _signal_sources(
    source_numbers: dict[str, tuple[str, ...]],
) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for source_name in ("hsca", "api"):
        for value in source_numbers.get(source_name, ()):
            out.append({"source": source_name, "value": value})
    return out


def _best_previous_name_annotation(
    company: CompanyCandidate,
    provider: ProviderCandidate,
) -> dict[str, object] | None:
    best_raw_name: str | None = None
    best_ratio = 0
    for raw_name, normalized_name in company.previous_names:
        ratio = _token_set_ratio(provider.provider_name_normalized, normalized_name)
        if ratio > best_ratio:
            best_ratio = ratio
            best_raw_name = raw_name
    if best_raw_name is None or best_ratio < MATCH_THRESHOLD:
        return None
    return {
        "signal": "prev_name_annotation",
        "value": best_raw_name,
        "ratio": best_ratio,
        "affects_total": False,
    }


def _aggressive_normalize_annotation(
    company: CompanyCandidate,
    provider: ProviderCandidate,
    *,
    standard_ratio: int,
) -> dict[str, object] | None:
    aggressive_ratio = _token_set_ratio(
        normalize_name(company.company_name, aggressive=True),
        normalize_name(provider.provider_name, aggressive=True),
    )
    if aggressive_ratio < MATCH_THRESHOLD or aggressive_ratio <= standard_ratio:
        return None
    return {
        "signal": "aggressive_normalize_annotation",
        "ratio": aggressive_ratio,
        "affects_total": False,
    }


def _no_postcode_annotation(
    company: CompanyCandidate,
    provider: ProviderCandidate,
) -> dict[str, object] | None:
    if company.outward_postcode and provider.postcode_prefixes:
        return None
    if not company.outward_postcode and not provider.postcode_prefixes:
        value = "company_and_provider_missing_postcode"
    elif not company.outward_postcode:
        value = "company_missing_postcode"
    else:
        value = "provider_missing_postcode"
    return {
        "signal": "no_postcode_annotation",
        "value": value,
        "affects_total": False,
    }


def _annotation_signals(
    company: CompanyCandidate,
    provider: ProviderCandidate,
    *,
    standard_ratio: int,
) -> list[dict[str, object]]:
    signals: list[dict[str, object]] = []
    previous_name_signal = _best_previous_name_annotation(company, provider)
    if previous_name_signal is not None:
        signals.append(previous_name_signal)

    no_postcode_signal = _no_postcode_annotation(company, provider)
    if no_postcode_signal is not None:
        signals.append(no_postcode_signal)

    aggressive_signal = _aggressive_normalize_annotation(
        company,
        provider,
        standard_ratio=standard_ratio,
    )
    if aggressive_signal is not None:
        signals.append(aggressive_signal)
    return signals


def _build_direct_match(
    company: CompanyCandidate,
    provider: ProviderCandidate,
    *,
    source_numbers: dict[str, tuple[str, ...]],
) -> MatchCandidate:
    all_numbers = {
        number
        for numbers in source_numbers.values()
        for number in numbers
    }
    conflict = len(all_numbers) > 1
    total_score = CONFLICT_SCORE if conflict else 100
    status = "needs_review" if conflict else "auto_confirmed"
    standard_ratio = _token_set_ratio(
        company.company_name_normalized,
        provider.provider_name_normalized,
    )
    match_signals: list[dict[str, object]] = [
        {
            "signal": "ch_number",
            "score": total_score,
            "affects_total": True,
            "matched_value": company.company_number,
            "sources": _signal_sources(source_numbers),
        }
    ]
    if conflict:
        match_signals.append(
            {
                "signal": "ch_number_conflict",
                "affects_total": False,
                "sources": _signal_sources(source_numbers),
            }
        )
    match_signals.extend(
        _annotation_signals(company, provider, standard_ratio=standard_ratio)
    )
    return MatchCandidate(
        company_number=company.company_number,
        cqc_provider_id=provider.provider_id,
        total_score=total_score,
        match_signals=tuple(match_signals),
        status=status,
    )


def _build_fuzzy_match(
    company: CompanyCandidate,
    provider: ProviderCandidate,
    *,
    ratio: int,
) -> MatchCandidate:
    shared_outward_codes = []
    if company.outward_postcode and company.outward_postcode in provider.postcode_prefixes:
        shared_outward_codes.append(company.outward_postcode)
    match_signals: list[dict[str, object]] = [
        {
            "signal": "fuzzy_name_outward_pc",
            "score": MATCH_THRESHOLD,
            "ratio": ratio,
            "affects_total": True,
            "shared_outward_postcodes": shared_outward_codes,
        }
    ]
    match_signals.extend(
        _annotation_signals(company, provider, standard_ratio=ratio)
    )
    return MatchCandidate(
        company_number=company.company_number,
        cqc_provider_id=provider.provider_id,
        total_score=MATCH_THRESHOLD,
        match_signals=tuple(match_signals),
        status="auto_confirmed",
    )


def _candidate_matches(
    companies: list[CompanyCandidate],
    providers: list[ProviderCandidate],
) -> list[MatchCandidate]:
    matches: list[MatchCandidate] = []

    companies_by_number = {company.company_number: company for company in companies}
    matched_company_numbers: set[str] = set()
    matched_provider_ids: set[str] = set()

    for provider in providers:
        source_numbers = _provider_source_numbers(provider)
        if not source_numbers:
            continue

        matching_company_numbers = _dedupe_preserve_order(
            [
                number
                for numbers in source_numbers.values()
                for number in numbers
                if number in companies_by_number
            ]
        )
        if not matching_company_numbers:
            continue

        for company_number in matching_company_numbers:
            company = companies_by_number[company_number]
            matches.append(
                _build_direct_match(
                    company,
                    provider,
                    source_numbers=source_numbers,
                )
            )
            matched_company_numbers.add(company_number)
        matched_provider_ids.add(provider.provider_id)

    companies_by_outward: dict[str, list[CompanyCandidate]] = {}
    for company in companies:
        if company.company_number in matched_company_numbers:
            continue
        if company.outward_postcode is None:
            continue
        companies_by_outward.setdefault(company.outward_postcode, []).append(company)

    fuzzy_matched_company_numbers: set[str] = set()
    for provider in providers:
        if provider.provider_id in matched_provider_ids:
            continue
        if not provider.provider_name_normalized or not provider.postcode_prefixes:
            continue

        candidates: list[CompanyCandidate] = []
        seen_company_numbers: set[str] = set()
        for outward_code in provider.postcode_prefixes:
            for company in companies_by_outward.get(outward_code, []):
                if company.company_number in fuzzy_matched_company_numbers:
                    continue
                if company.company_number in seen_company_numbers:
                    continue
                seen_company_numbers.add(company.company_number)
                candidates.append(company)

        best_company: CompanyCandidate | None = None
        best_ratio = 0
        for company in candidates:
            ratio = _token_set_ratio(
                company.company_name_normalized,
                provider.provider_name_normalized,
            )
            if ratio > best_ratio:
                best_ratio = ratio
                best_company = company

        if best_company is None or best_ratio < MATCH_THRESHOLD:
            continue

        matches.append(
            _build_fuzzy_match(
                best_company,
                provider,
                ratio=best_ratio,
            )
        )
        fuzzy_matched_company_numbers.add(best_company.company_number)
        matched_provider_ids.add(provider.provider_id)

    return matches


def _existing_pair_statuses(
    con: duckdb.DuckDBPyConnection,
) -> dict[tuple[str, str], str]:
    rows = con.execute(
        """
        SELECT company_number, cqc_provider_id, status
        FROM ch_cqc_matches
        """
    ).fetchall()
    return {
        (str(company_number), str(provider_id)): str(status)
        for company_number, provider_id, status in rows
    }


def _delete_non_user_rows(
    con: duckdb.DuckDBPyConnection,
    *,
    company_numbers: tuple[str, ...] | None,
    provider_ids: tuple[str, ...] | None,
) -> None:
    conditions = ["status NOT IN ('user_confirmed', 'user_rejected')"]
    params: list[object] = []
    scope_conditions: list[str] = []
    if company_numbers:
        placeholders = ", ".join(["?"] * len(company_numbers))
        scope_conditions.append(f"company_number IN ({placeholders})")
        params.extend(company_numbers)
    if provider_ids:
        placeholders = ", ".join(["?"] * len(provider_ids))
        scope_conditions.append(f"cqc_provider_id IN ({placeholders})")
        params.extend(provider_ids)
    if scope_conditions:
        conditions.append("(" + " OR ".join(scope_conditions) + ")")

    con.execute(
        f"DELETE FROM ch_cqc_matches WHERE {' AND '.join(conditions)}",
        params,
    )


def _insert_matches(
    con: duckdb.DuckDBPyConnection,
    matches: list[MatchCandidate],
) -> None:
    matched_at = utcnow_naive()
    for match in matches:
        con.execute(
            """
            INSERT OR REPLACE INTO ch_cqc_matches (
                company_number,
                cqc_provider_id,
                total_score,
                match_signals,
                status,
                matched_at
            )
            VALUES (?, ?, ?, CAST(? AS JSON), ?, ?)
            """,
            [
                match.company_number,
                match.cqc_provider_id,
                match.total_score,
                _json_text(list(match.match_signals)),
                match.status,
                matched_at,
            ],
        )


def match_companies_to_cqc(
    db_path: str | Path,
    *,
    mode: str = "incremental",
    company_numbers: list[str] | None = None,
    provider_ids: list[str] | None = None,
) -> dict[str, int | str]:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    normalized_company_numbers = (
        list(
            _dedupe_preserve_order(
                [
                    company_number
                    for company_number in (
                        normalize_company_number(value)
                        for value in (company_numbers or [])
                    )
                    if company_number is not None
                ]
            )
        )
        or None
    )
    normalized_provider_ids = (
        list(
            _dedupe_preserve_order(
                [str(value).strip() for value in (provider_ids or []) if str(value).strip()]
            )
        )
        or None
    )

    mode_value = _validated_mode(mode)
    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        ensure_pipeline_schema(con)
        _require_match_inputs(con)

        companies = _select_companies(
            con,
            company_numbers=normalized_company_numbers,
            provider_ids=normalized_provider_ids,
        )
        providers = _select_providers(
            con,
            provider_ids=normalized_provider_ids,
        )
        matches = _candidate_matches(companies, providers)
        existing_statuses = _existing_pair_statuses(con)
        existing_pairs = set(existing_statuses)
        existing_user_pairs = {
            pair
            for pair, status in existing_statuses.items()
            if status in USER_LOCKED_STATUSES
        }

        if mode_value == "all":
            matches_to_write = [
                match
                for match in matches
                if (match.company_number, match.cqc_provider_id) not in existing_user_pairs
            ]
        else:
            matches_to_write = [
                match
                for match in matches
                if (match.company_number, match.cqc_provider_id) not in existing_pairs
            ]

        con.execute("BEGIN TRANSACTION")
        try:
            if mode_value == "all":
                _delete_non_user_rows(
                    con,
                    company_numbers=(
                        tuple(normalized_company_numbers)
                        if normalized_company_numbers is not None
                        else None
                    ),
                    provider_ids=(
                        tuple(normalized_provider_ids)
                        if normalized_provider_ids is not None
                        else None
                    ),
                )
            _insert_matches(con, matches_to_write)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        auto_confirmed = sum(1 for match in matches if match.status == "auto_confirmed")
        needs_review = sum(1 for match in matches if match.status == "needs_review")
        preserved_user_rows = sum(
            1
            for match in matches
            if (match.company_number, match.cqc_provider_id) in existing_user_pairs
        )
        if mode_value == "all":
            skipped_existing_rows = preserved_user_rows
        else:
            skipped_existing_rows = sum(
                1
                for match in matches
                if (match.company_number, match.cqc_provider_id) in existing_pairs
            )

        logger.info(
            "CQC matcher complete: mode=%s companies=%d providers=%d matches=%d written=%d auto=%d review=%d preserved_user=%d",
            mode_value,
            len(companies),
            len(providers),
            len(matches),
            len(matches_to_write),
            auto_confirmed,
            needs_review,
            preserved_user_rows,
        )
        return {
            "mode": mode_value,
            "companies_considered": len(companies),
            "providers_considered": len(providers),
            "matches_found": len(matches),
            "written": len(matches_to_write),
            "auto_confirmed": auto_confirmed,
            "needs_review": needs_review,
            "preserved_user_rows": preserved_user_rows,
            "skipped_existing_rows": skipped_existing_rows,
        }
    finally:
        con.close()
