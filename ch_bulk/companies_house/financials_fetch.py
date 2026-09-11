"""Companies House fetch and selection helpers for financial enrichment."""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Any

import duckdb
import requests
from requests.adapters import HTTPAdapter

from ch_bulk.companies_house.financials_contracts import (
    FilingCandidate,
    FinancialTarget,
    FetchedFinancialRow,
    FetchedFinancialWorkItem,
    IXBRL_EXTENSION,
    IXBRL_RESOURCE,
    PDF_EXTENSION,
    PDF_RESOURCE,
    FILED_REVENUE_SOURCES,
    TERMINAL_REVENUE_SOURCES,
)
from ch_bulk.core.cancellation import OperationCancelled, cancellable_sleep
from ch_bulk.companies_house.financials_parsers import _parse_date
from ch_bulk.core.paths import raw_filings_dir
from ch_bulk.core.rate_limit import SlidingWindowThrottle
from ch_bulk.db.staging import isoformat_utc

logger = logging.getLogger(__name__)

CH_API_BASE = "https://api.company-information.service.gov.uk"
DOCUMENT_API_BASE = "https://document-api.company-information.service.gov.uk"

DEFAULT_RETRY_AFTER_SECONDS = 60
# Stay below the published 600/5min ceiling so small bursts and clock skew do
# not trip a 429/ban cycle on a long unattended run.
EFFECTIVE_CH_MAX_REQUESTS = 550
CH_WINDOW_SECONDS = 300

ANNUAL_ACCOUNTS_TYPES = {"AA", "AAMD"}
ANNUAL_ACCOUNTS_DESCRIPTION_PREFIXES = (
    "accounts-with-accounts-type-",
    "accounts-amended-with-accounts-type-",
)


class CompaniesHouseFinancialsClient:
    def __init__(
        self,
        *,
        api_key: str,
        throttle: SlidingWindowThrottle,
    ) -> None:
        self._api_key = api_key
        self._throttle = throttle
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "ch-bulk-financials"})
        adapter = HTTPAdapter(max_retries=0, pool_connections=8, pool_maxsize=8)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "CompaniesHouseFinancialsClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _get(
        self,
        url: str,
        *,
        params: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
        allow_redirects: bool = True,
        auth: bool = True,
        cancel_event: threading.Event | None = None,
    ) -> requests.Response:
        for attempt in range(5):
            if cancel_event is None:
                self._throttle.wait()
            else:
                self._throttle.wait(cancel_event=cancel_event)
            response = self._session.get(
                url,
                params=params,
                headers=headers,
                timeout=60,
                allow_redirects=allow_redirects,
                auth=(self._api_key, "") if auth else None,
            )
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                try:
                    pause = (
                        int(retry_after)
                        if retry_after is not None
                        else DEFAULT_RETRY_AFTER_SECONDS
                    )
                except ValueError:
                    pause = DEFAULT_RETRY_AFTER_SECONDS
                logger.warning("Companies House 429 for %s, sleeping %ss", url, pause)
                cancellable_sleep(
                    cancel_event,
                    pause,
                    reason=f"financials request cancelled: {url}",
                    sleep_fn=time.sleep,
                )
                continue
            if response.status_code in {502, 503, 504}:
                pause = 2**attempt
                logger.warning(
                    "Companies House %s for %s, retrying in %ss",
                    response.status_code,
                    url,
                    pause,
                )
                cancellable_sleep(
                    cancel_event,
                    pause,
                    reason=f"financials request cancelled: {url}",
                    sleep_fn=time.sleep,
                )
                continue
            response.raise_for_status()
            return response
        raise RuntimeError(f"Companies House request failed after retries: {url}")

    def get_filing_history(
        self,
        company_number: str,
        *,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        request_kwargs: dict[str, object] = {
            "params": {"category": "accounts", "items_per_page": 100}
        }
        if cancel_event is not None:
            request_kwargs["cancel_event"] = cancel_event
        response = self._get(
            f"{CH_API_BASE}/company/{company_number}/filing-history",
            **request_kwargs,
        )
        return dict(response.json())

    def get_document_metadata(
        self,
        document_metadata_url: str,
        *,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        if cancel_event is None:
            response = self._get(document_metadata_url)
        else:
            response = self._get(document_metadata_url, cancel_event=cancel_event)
        return dict(response.json())

    def _response_bytes_with_retries(
        self,
        *,
        url: str,
        request_factory: Any,
        cancel_event: threading.Event | None = None,
    ) -> bytes:
        for attempt in range(5):
            try:
                response = request_factory()
                response.raise_for_status()
                return bytes(response.content)
            except requests.HTTPError:
                raise
            except requests.RequestException as exc:
                pause = 2**attempt
                if attempt == 4:
                    raise
                logger.warning(
                    "Document body download failed for %s (%s), retrying in %ss",
                    url,
                    type(exc).__name__,
                    pause,
                )
                cancellable_sleep(
                    cancel_event,
                    pause,
                    reason=f"financials document retry cancelled: {url}",
                    sleep_fn=time.sleep,
                )
        raise RuntimeError(f"Document download failed after retries: {url}")

    def download_document(
        self,
        *,
        document_url: str,
        accept: str,
        cancel_event: threading.Event | None = None,
    ) -> bytes:
        request_kwargs: dict[str, object] = {
            "headers": {"Accept": accept},
            "allow_redirects": False,
        }
        if cancel_event is not None:
            request_kwargs["cancel_event"] = cancel_event
        response = self._get(document_url, **request_kwargs)
        if response.status_code in {301, 302, 303, 307, 308}:
            redirect_url = response.headers.get("Location")
            if not redirect_url:
                raise RuntimeError(f"Document redirect missing Location: {document_url}")
            return self._response_bytes_with_retries(
                url=redirect_url,
                request_factory=lambda: self._session.get(
                    redirect_url,
                    timeout=60,
                ),
                cancel_event=cancel_event,
            )
        response_holder = [response]
        return self._response_bytes_with_retries(
            url=document_url,
            request_factory=lambda: (
                response_holder.pop()
                if response_holder
                else self._get(
                    document_url,
                    headers={"Accept": accept},
                    allow_redirects=False,
                    **({"cancel_event": cancel_event} if cancel_event is not None else {}),
                )
            ),
            cancel_event=cancel_event,
        )

    def download_document_content(
        self,
        *,
        document_metadata_url: str,
        accept: str,
        cancel_event: threading.Event | None = None,
    ) -> bytes:
        request_kwargs: dict[str, object] = {
            "document_url": f"{document_metadata_url.rstrip('/')}/content",
            "accept": accept,
        }
        if cancel_event is not None:
            request_kwargs["cancel_event"] = cancel_event
        return self.download_document(**request_kwargs)


def _http_status_from_exception(exc: requests.HTTPError) -> int | None:
    response = exc.response
    return response.status_code if response is not None else None


def _normalize_company_ids(values: list[str] | None) -> list[str]:
    if not values:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values:
        cleaned = raw.strip().zfill(8)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)
    return normalized


def _filing_extension_for_format(filing_format: str | None) -> str | None:
    normalized = str(filing_format or "").strip().lower()
    if normalized == "ixbrl":
        return IXBRL_EXTENSION
    if normalized == "pdf":
        return PDF_EXTENSION
    return None


def _raw_filing_path(
    *,
    data_dir: str | Path,
    company_number: str,
    filing_id: str | None,
    filing_format: str | None,
) -> Path | None:
    if not filing_id:
        return None
    filings_dir = raw_filings_dir(data_dir, company_number)
    extension = _filing_extension_for_format(filing_format)
    if extension is not None:
        path = filings_dir / f"{filing_id}.{extension}"
        return path if path.exists() else None

    for candidate_extension in (IXBRL_EXTENSION, PDF_EXTENSION):
        path = filings_dir / f"{filing_id}.{candidate_extension}"
        if path.exists():
            return path
    return None


def _select_targets(
    con: duckdb.DuckDBPyConnection,
    *,
    mode: str,
    ids: list[str] | None,
    data_dir: str | Path | None = None,
) -> list[FinancialTarget]:
    if mode == "list":
        cleaned = _normalize_company_ids(ids)
        if not cleaned:
            raise ValueError("mode=list requires one or more ids")
        placeholders = ", ".join(["?"] * len(cleaned))
        rows = con.execute(
            f"""
            SELECT company_number, accounts_last_made_up
            FROM companies
            WHERE company_number IN ({placeholders})
            ORDER BY company_number
            """,
            cleaned,
        ).fetchall()
        return [FinancialTarget(row[0], row[1]) for row in rows]

    joins = ""
    filters = ""
    select_extra = "NULL AS filing_id, NULL AS filing_format"
    if mode == "incremental":
        joins = "LEFT JOIN company_enrichment ce USING (company_number)"
        select_extra = """
            NULLIF(CAST(ce.filing_id AS VARCHAR), '') AS filing_id,
            NULLIF(CAST(ce.filing_format AS VARCHAR), '') AS filing_format
        """
        filters = f"""
          AND COALESCE(ce.revenue_source, '') NOT IN ({_terminal_sources_sql()})
          AND NOT (
                ce.revenue IS NOT NULL
            AND ce.revenue_source IN ({_filed_sources_sql()})
          )
        """

    rows = con.execute(
        f"""
        SELECT DISTINCT
            c.company_number,
            c.accounts_last_made_up,
            {select_extra}
        FROM current_company_match m
        JOIN companies c USING (company_number)
        {joins}
        WHERE m.status IN ('user_confirmed', 'auto_confirmed', 'needs_review')
        {filters}
        ORDER BY c.company_number
        """
    ).fetchall()
    targets: list[FinancialTarget] = []
    for company_number, accounts_last_made_up, filing_id, filing_format in rows:
        if mode == "incremental" and data_dir is not None:
            raw_path = _raw_filing_path(
                data_dir=data_dir,
                company_number=str(company_number),
                filing_id=str(filing_id) if filing_id is not None else None,
                filing_format=str(filing_format) if filing_format is not None else None,
            )
            if raw_path is not None:
                continue
        targets.append(FinancialTarget(company_number, accounts_last_made_up))
    return targets


def _select_latest_annual_accounts(
    filing_history: dict[str, Any],
) -> FilingCandidate | None:
    for item in filing_history.get("items", []):
        links = item.get("links") or {}
        document_metadata_url = str(links.get("document_metadata") or "").strip()
        if not document_metadata_url:
            continue
        filing_type = str(item.get("type") or "").upper()
        description = str(item.get("description") or "")
        if (
            filing_type not in ANNUAL_ACCOUNTS_TYPES
            and not description.startswith(ANNUAL_ACCOUNTS_DESCRIPTION_PREFIXES)
        ):
            continue
        filing_id = str(item.get("transaction_id") or "").strip()
        if not filing_id:
            continue
        return FilingCandidate(
            filing_id=filing_id,
            filing_date=_parse_date(item.get("date")),
            made_up_date=_parse_date(
                (item.get("description_values") or {}).get("made_up_date")
            ),
            paper_filed=bool(item.get("paper_filed")),
            document_metadata_url=document_metadata_url,
        )
    return None


def _save_raw_filing(
    *,
    data_dir: str | Path,
    company_number: str,
    filing_id: str,
    extension: str,
    content: bytes,
) -> Path:
    filings_dir = raw_filings_dir(data_dir, company_number)
    filings_dir.mkdir(parents=True, exist_ok=True)
    target_path = filings_dir / f"{filing_id}.{extension}"
    with open(target_path, "wb") as handle:
        handle.write(content)
    return target_path


def _terminal_sources_sql() -> str:
    """Render TERMINAL_REVENUE_SOURCES as a SQL IN-list.

    Derived from the constant rather than restated inline so the set cannot
    drift from the one the enricher applies.
    """
    return ", ".join(f"'{source}'" for source in sorted(TERMINAL_REVENUE_SOURCES))


def _filed_sources_sql() -> str:
    """Render FILED_REVENUE_SOURCES as a SQL IN-list."""
    return ", ".join(f"'{source}'" for source in sorted(FILED_REVENUE_SOURCES))


def _document_bytes(
    *,
    client: Any,
    data_dir: str | Path,
    company_number: str,
    filing_id: str,
    document_metadata_url: str,
    accept: str,
    filing_format: str,
    cancel_event: threading.Event | None,
) -> tuple[bytes, Path]:
    """Return the filing's bytes, reusing a previously saved copy if present.

    A Companies House document is immutable for a given filing_id, so a file
    already on disk is always safe to serve. Only _select_targets consulted the
    saved copies before, and only in incremental mode, which meant mode=list
    and mode=all re-downloaded documents we already had -- the expensive case
    being multi-megabyte scanned PDFs.

    Raises whatever the client raises on a failed download, so callers keep
    their existing HTTPError handling.
    """
    cached = _raw_filing_path(
        data_dir=data_dir,
        company_number=company_number,
        filing_id=filing_id,
        filing_format=filing_format,
    )
    if cached is not None:
        logger.debug("Reusing saved filing %s", cached)
        return cached.read_bytes(), cached

    request_kwargs: dict[str, object] = {
        "document_metadata_url": document_metadata_url,
        "accept": accept,
    }
    if cancel_event is not None:
        request_kwargs["cancel_event"] = cancel_event
    content = client.download_document_content(**request_kwargs)

    extension = _filing_extension_for_format(filing_format)
    assert extension is not None, f"unknown filing_format: {filing_format}"
    saved = _save_raw_filing(
        data_dir=data_dir,
        company_number=company_number,
        filing_id=filing_id,
        extension=extension,
        content=content,
    )
    return content, saved


def _build_fetched_row(
    *,
    target: FinancialTarget,
    filing: FilingCandidate | None,
    paper_filed: bool | None,
    filing_format: str | None,
    raw_path: str | None,
    parse_status: str | None,
    parse_failure_reason: str | None,
) -> FetchedFinancialRow:
    return FetchedFinancialRow(
        company_number=target.company_number,
        accounts_last_made_up=(
            target.accounts_last_made_up.isoformat()
            if target.accounts_last_made_up
            else None
        ),
        filing_id=filing.filing_id if filing else None,
        filing_date=(
            filing.filing_date.isoformat()
            if filing and filing.filing_date
            else None
        ),
        filing_made_up_date=(
            filing.made_up_date.isoformat()
            if filing and filing.made_up_date
            else None
        ),
        paper_filed=paper_filed,
        filing_format=filing_format,
        raw_path=raw_path,
        parse_status=parse_status,
        parse_failure_reason=parse_failure_reason,
        fetched_at=isoformat_utc(),
    )


def _fetch_company_work_item(
    client: CompaniesHouseFinancialsClient,
    *,
    target: FinancialTarget,
    data_dir: str | Path,
    cancel_event: threading.Event | None = None,
) -> FetchedFinancialWorkItem:
    started = time.monotonic()
    http_status: int | None = None
    try:
        if cancel_event is None:
            filing_history = client.get_filing_history(target.company_number)
        else:
            filing_history = client.get_filing_history(
                target.company_number,
                cancel_event=cancel_event,
            )
        http_status = 200
        filing = _select_latest_annual_accounts(filing_history)
        if filing is None:
            return FetchedFinancialWorkItem(
                row=_build_fetched_row(
                    target=target,
                    filing=None,
                    paper_filed=None,
                    filing_format=None,
                    raw_path=None,
                    parse_status="no_filing",
                    parse_failure_reason="no_annual_accounts_filing",
                ),
                http_status=http_status,
                started_monotonic=started,
            )

        if filing.paper_filed:
            try:
                content, raw_path = _document_bytes(
                    client=client,
                    data_dir=data_dir,
                    company_number=target.company_number,
                    filing_id=filing.filing_id,
                    document_metadata_url=filing.document_metadata_url,
                    accept=PDF_RESOURCE,
                    filing_format="pdf",
                    cancel_event=cancel_event,
                )
            except requests.HTTPError as exc:
                http_status = _http_status_from_exception(exc) or http_status
                return FetchedFinancialWorkItem(
                    row=_build_fetched_row(
                        target=target,
                        filing=filing,
                        paper_filed=filing.paper_filed,
                        filing_format="pdf",
                        raw_path=None,
                        parse_status="document_download_error",
                        parse_failure_reason=(
                            f"http_status_{http_status}"
                            if http_status is not None
                            else "http_error"
                        ),
                    ),
                    http_status=http_status,
                    started_monotonic=started,
                )
            return FetchedFinancialWorkItem(
                row=_build_fetched_row(
                    target=target,
                    filing=filing,
                    paper_filed=filing.paper_filed,
                    filing_format="pdf",
                    raw_path=str(raw_path),
                    parse_status="pdf_no_text_layer",
                    parse_failure_reason="pdf_no_text_layer",
                ),
                http_status=http_status,
                started_monotonic=started,
            )

        try:
            content, raw_path = _document_bytes(
                client=client,
                data_dir=data_dir,
                company_number=target.company_number,
                filing_id=filing.filing_id,
                document_metadata_url=filing.document_metadata_url,
                accept=IXBRL_RESOURCE,
                filing_format="ixbrl",
                cancel_event=cancel_event,
            )
        except requests.HTTPError as exc:
            http_status = _http_status_from_exception(exc) or http_status
            if http_status == 406:
                try:
                    content, raw_path = _document_bytes(
                        client=client,
                        data_dir=data_dir,
                        company_number=target.company_number,
                        filing_id=filing.filing_id,
                        document_metadata_url=filing.document_metadata_url,
                        accept=PDF_RESOURCE,
                        filing_format="pdf",
                        cancel_event=cancel_event,
                    )
                except requests.HTTPError as pdf_exc:
                    http_status = _http_status_from_exception(pdf_exc) or http_status
                    return FetchedFinancialWorkItem(
                        row=_build_fetched_row(
                            target=target,
                            filing=filing,
                            paper_filed=filing.paper_filed,
                            filing_format="pdf",
                            raw_path=None,
                            parse_status="document_download_error",
                            parse_failure_reason=(
                                f"http_status_{http_status}"
                                if http_status is not None
                                else "http_error"
                            ),
                        ),
                        http_status=http_status,
                        started_monotonic=started,
                    )
                return FetchedFinancialWorkItem(
                    row=_build_fetched_row(
                        target=target,
                        filing=filing,
                        paper_filed=filing.paper_filed,
                        filing_format="pdf",
                        raw_path=str(raw_path),
                        parse_status="pdf_no_text_layer",
                        parse_failure_reason="pdf_no_text_layer",
                    ),
                    http_status=http_status,
                    started_monotonic=started,
                )

            return FetchedFinancialWorkItem(
                row=_build_fetched_row(
                    target=target,
                    filing=filing,
                    paper_filed=filing.paper_filed,
                    filing_format="ixbrl",
                    raw_path=None,
                    parse_status="document_download_error",
                    parse_failure_reason=(
                        f"http_status_{http_status}"
                        if http_status is not None
                        else "http_error"
                    ),
                ),
                http_status=http_status,
                started_monotonic=started,
            )
        return FetchedFinancialWorkItem(
            row=_build_fetched_row(
                target=target,
                filing=filing,
                paper_filed=filing.paper_filed,
                filing_format="ixbrl",
                raw_path=str(raw_path),
                parse_status=None,
                parse_failure_reason=None,
            ),
            http_status=http_status,
            started_monotonic=started,
        )
    except OperationCancelled:
        raise
    except requests.HTTPError as exc:
        http_status = _http_status_from_exception(exc) or http_status
        return FetchedFinancialWorkItem(
            row=_build_fetched_row(
                target=target,
                filing=None,
                paper_filed=None,
                filing_format=None,
                raw_path=None,
                parse_status="request_error",
                parse_failure_reason=(
                    f"http_status_{http_status}"
                    if http_status is not None
                    else "http_error"
                ),
            ),
            http_status=http_status,
            started_monotonic=started,
        )
    except Exception as exc:
        logger.exception("Financials enrich failed for %s", target.company_number)
        return FetchedFinancialWorkItem(
            row=_build_fetched_row(
                target=target,
                filing=None,
                paper_filed=None,
                filing_format=None,
                raw_path=None,
                parse_status="request_error",
                parse_failure_reason=str(exc),
            ),
            http_status=http_status,
            started_monotonic=started,
        )
