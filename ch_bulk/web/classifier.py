"""Website classification pipeline with JSONL staging and LLM calls."""

from __future__ import annotations

import ast
import hashlib
import json
import logging
import os
import queue
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urljoin, urlsplit, urlunsplit
from uuid import uuid4

import duckdb
import httpx
import requests
import trafilatura
from requests.adapters import HTTPAdapter

from ch_bulk.web import browser
from ch_bulk.core.logging import FsyncLineLogger
from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.core.paths import DEFAULT_DATA_DIR, DEFAULT_DB_PATH
from ch_bulk.core.settings import load_settings
from ch_bulk.db.staging import (
    LoadedBatch,
    STAGED_JSONL_SCAN_SQL,
    isoformat_utc,
    mark_staging_file_loaded,
    parse_staged_datetime,
    with_duckdb_connection,
)

logger = logging.getLogger(__name__)

Mode = Literal["incremental", "all", "list"]
CLASSIFICATION_SYNC_TYPE = "classifications"
DEFAULT_BATCH_SIZE = 100
USER_AGENT = "Mozilla/5.0 UK-Homecare-Toolkit"
MIN_CLASSIFIABLE_TEXT_LEN = 200
PAGE_PATHS = (
    "",
    "services",
    "about",
    "what-we-do",
    "care-services",
    "our-services",
    "our-care",
)
VALID_VERDICTS = {
    "Majority domiciliary",
    "Majority Supported living",
    "Majority residential",
    "Mixed_domiciliary_supported",
    "Mixed_residential_domiciliary",
    "Unable to classify",
}
CLASSIFICATION_FILENAME_PREFIX = f"{CLASSIFICATION_SYNC_TYPE}_"
LLM_REQUEST_FAILURE_REASON = "llm_request_error"
CLASSIFICATION_ERROR_REASONS = {
    "all_pages_unreachable",
    LLM_REQUEST_FAILURE_REASON,
    "parse_error",
}
VERDICT_NORMALIZATION = {
    "majority domiciliary care": "Majority domiciliary",
    "majority domiciliary": "Majority domiciliary",
    "majority supported living": "Majority Supported living",
    "majority residential": "Majority residential",
    "mixed_domiciliary_supported": "Mixed_domiciliary_supported",
    "mixed residential domiciliary": "Mixed_residential_domiciliary",
    "mixed_residential_domiciliary": "Mixed_residential_domiciliary",
    "unable to classify": "Unable to classify",
}
VERDICT_SQL_EXPRESSION = """
CASE
    WHEN lower(trim(coalesce(raw_json->>'verdict', ''))) = 'majority domiciliary care'
    THEN 'Majority domiciliary'
    WHEN lower(trim(coalesce(raw_json->>'verdict', ''))) = 'majority domiciliary'
    THEN 'Majority domiciliary'
    WHEN lower(trim(coalesce(raw_json->>'verdict', ''))) = 'majority supported living'
    THEN 'Majority Supported living'
    WHEN lower(trim(coalesce(raw_json->>'verdict', ''))) = 'majority residential'
    THEN 'Majority residential'
    WHEN lower(trim(coalesce(raw_json->>'verdict', ''))) = 'mixed_domiciliary_supported'
    THEN 'Mixed_domiciliary_supported'
    WHEN lower(trim(coalesce(raw_json->>'verdict', ''))) = 'mixed residential domiciliary'
    THEN 'Mixed_residential_domiciliary'
    WHEN lower(trim(coalesce(raw_json->>'verdict', ''))) = 'mixed_residential_domiciliary'
    THEN 'Mixed_residential_domiciliary'
    WHEN lower(trim(coalesce(raw_json->>'verdict', ''))) = 'unable to classify'
    THEN 'Unable to classify'
    ELSE coalesce(nullif(trim(raw_json->>'verdict'), ''), 'Unable to classify')
END
"""
CLASSIFICATION_INSERT_SQL = f"""
INSERT OR REPLACE INTO classifications (
    company_number,
    source_type,
    verdict,
    verdict_reason,
    evidence_quote,
    source_url,
    classifier,
    classified_at,
    batch_id
)
SELECT
    entity_id AS company_number,
    'website' AS source_type,
    {VERDICT_SQL_EXPRESSION} AS verdict,
    raw_json->>'failure_reason' AS verdict_reason,
    raw_json->>'evidence' AS evidence_quote,
    raw_json->>'source_url' AS source_url,
    coalesce(raw_json->>'classifier', 'llm:unknown') AS classifier,
    CAST(fetched_at AS TIMESTAMP) AS classified_at,
    CAST(? AS UUID) AS batch_id
FROM read_json_auto(?, format='newline_delimited')
"""
CLASSIFICATION_BATCH_AGGREGATE_SQL = """
SELECT
    COUNT(*) AS records_updated,
    COUNT(*) FILTER (
        WHERE lower(trim(coalesce(verdict, ''))) = 'unable to classify'
    ) AS unable_count,
COUNT(*) FILTER (
    WHERE lower(trim(coalesce(verdict_reason, ''))) IN (
        'llm_request_error',
        'parse_error',
        'all_pages_unreachable'
    )
) AS error_count
FROM classifications
WHERE batch_id = CAST(? AS UUID)
"""
PROMPT_TEMPLATE = """You are classifying UK homecare company websites for an M&A targeting exercise.

Choose ONE verdict that best reflects the PRIMARY business:
- 'Majority domiciliary' — care delivered in the client's own home (visiting care, live-in)
- 'Majority Supported living' — clients live in their own tenancy with on-site/visiting support, typically learning disabilities
- 'Majority residential' — care home where clients live full-time
- 'Mixed_domiciliary_supported' — roughly equal mix of domiciliary + supported living
- 'Mixed_residential_domiciliary' — roughly equal mix of residential + domiciliary
- 'Unable to classify' — insufficient information

Site content (markdown):
<<<
{content}
>>>

Return JSON only: {{"verdict": "<one of above>", "evidence": "<one short quote from the content>"}}"""


@dataclass(frozen=True)
class PageFetch:
    path: str
    url: str
    status: int | None
    html: str | None
    markdown: str | None
    used_playwright: bool = False
    fetch_error: str | None = None

    @property
    def text_len(self) -> int:
        return len(self.markdown or "")


@dataclass(frozen=True)
class SiteContent:
    source_url: str | None
    pages: list[PageFetch]
    content: str
    truncated: bool
    used_playwright: bool
    failure_reason: str | None
    http_status: int | None
    retry_pages: tuple[tuple[str, str], ...] = ()

    @property
    def text_len(self) -> int:
        return len(self.content)


@dataclass(frozen=True)
class StagedClassification:
    entity_id: str
    entity_type: str
    fetched_at: str
    http_status: int | None
    raw_json: dict[str, Any]

    @classmethod
    def from_json_line(cls, line: str) -> "StagedClassification":
        payload = json.loads(line)
        http_status = payload.get("http_status")
        return cls(
            entity_id=str(payload["entity_id"]),
            entity_type=str(payload["entity_type"]),
            fetched_at=str(payload["fetched_at"]),
            http_status=int(http_status) if http_status is not None else None,
            raw_json=dict(payload["raw_json"]),
        )

    def to_json_line(self) -> str:
        return json.dumps(
            {
                "entity_id": self.entity_id,
                "entity_type": self.entity_type,
                "fetched_at": self.fetched_at,
                "http_status": self.http_status,
                "raw_json": self.raw_json,
            },
            ensure_ascii=True,
            sort_keys=True,
        )

    def fetched_at_value(self) -> datetime:
        return parse_staged_datetime(self.fetched_at)


@dataclass(frozen=True)
class StagedClassificationFileStats:
    total_rows: int
    invalid_entity_rows: int
    invalid_payload_rows: int


@dataclass(frozen=True)
class FallbackTask:
    company_number: str
    site: SiteContent
    fetch_latency: float


class ClassificationChunkWriter:
    """Write classifier rows to chunked JSONL files and finalize closed chunks."""

    def __init__(
        self,
        data_dir: str | Path,
        *,
        batch_id: str,
        lane: str,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.batch_id = batch_id
        self.lane = lane
        self.stage_dir = self.data_dir / "staging"
        self.stage_dir.mkdir(parents=True, exist_ok=True)
        self._part = 1
        self._path: Path | None = None
        self._handle: Any | None = None
        self._rows_in_file = 0

    @property
    def rows_in_file(self) -> int:
        return self._rows_in_file

    def _next_path(self) -> Path:
        return self.stage_dir / (
            f"{CLASSIFICATION_FILENAME_PREFIX}{self.batch_id}"
            f"__{self.lane}_p{self._part:04d}.jsonl.open"
        )

    def _ensure_handle(self) -> None:
        if self._handle is not None:
            return
        self._path = self._next_path()
        self._handle = open(
            self._path,
            "a",
            encoding="utf-8",
            buffering=1,
        )

    def append(self, row: StagedClassification) -> None:
        self._ensure_handle()
        assert self._handle is not None
        self._handle.write(f"{row.to_json_line()}\n")
        self._handle.flush()
        self._rows_in_file += 1

    def flush_and_fsync(self) -> None:
        if self._handle is None:
            return
        self._handle.flush()
        os.fsync(self._handle.fileno())

    def finalize(self) -> Path | None:
        if self._handle is None or self._path is None:
            return None
        self.flush_and_fsync()
        self._handle.close()
        ready_path = Path(str(self._path)[:-5])
        if ready_path.exists():
            ready_path.unlink()
        self._path.rename(ready_path)
        self._handle = None
        self._path = None
        self._rows_in_file = 0
        self._part += 1
        return ready_path

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None


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
    return f"{done}/{total} {(done / total) * 100:.1f}%"


def _duration_text(seconds: float) -> str:
    rounded = max(0, int(round(seconds)))
    minutes, secs = divmod(rounded, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def _normalize_url(url: str) -> str:
    cleaned = (url or "").strip()
    if not cleaned:
        return ""
    if "://" not in cleaned:
        cleaned = f"https://{cleaned}"
    parts = urlsplit(cleaned)
    if not parts.netloc:
        return cleaned
    path = parts.path or ""
    return urlunsplit((parts.scheme or "https", parts.netloc, path, parts.query, parts.fragment))


def _page_url(base_url: str, path: str) -> str:
    normalized = _normalize_url(base_url)
    if not normalized:
        return normalized
    if not path:
        return normalized
    return urljoin(normalized.rstrip("/") + "/", path)


def _strip_https_www(url: str) -> str | None:
    if not url.startswith("https://www."):
        return None
    return "https://" + url[len("https://www.") :]


def _extract_markdown(html: str | None) -> str | None:
    if not html:
        return None
    try:
        return trafilatura.extract(
            html,
            output_format="markdown",
            include_tables=True,
            deduplicate=True,
        )
    except Exception:
        logger.exception("Failed to extract markdown")
        return None


def _canonical_verdict(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    return VERDICT_NORMALIZATION.get(normalized.lower(), normalized)


def _extract_text_content(message: dict[str, Any]) -> str:
    content = message.get("content")
    if isinstance(content, str) and content.strip():
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text = item.get("text")
                if text:
                    parts.append(str(text))
        joined = "\n".join(parts).strip()
        if joined:
            return joined
    reasoning = message.get("reasoning_content")
    if isinstance(reasoning, str):
        return reasoning
    return ""


def _parse_json_object_candidate(raw: str) -> dict[str, Any] | None:
    candidate = raw.strip()
    if not candidate:
        return None
    try:
        payload = json.loads(candidate)
    except json.JSONDecodeError:
        try:
            payload = ast.literal_eval(candidate)
        except (SyntaxError, ValueError):
            return None
    if isinstance(payload, dict) and "verdict" in payload:
        return payload
    return None


def _extract_json_object(text: str) -> dict[str, Any] | None:
    for match in re.finditer(r"\{", text):
        payload = _parse_json_object_candidate(text[match.start() :])
        if payload is not None:
            return payload
    return None


def _batch_error_delta(row: StagedClassification) -> int:
    failure_reason = str(row.raw_json.get("failure_reason") or "").strip().lower()
    if failure_reason in CLASSIFICATION_ERROR_REASONS:
        return 1
    if row.raw_json.get("error"):
        return 1
    return 0


def _classification_batch_id_from_path(path: str | Path) -> str:
    name = Path(path).name
    if name.endswith(".open"):
        name = name[:-5]
    if not name.startswith(CLASSIFICATION_FILENAME_PREFIX) or not name.endswith(".jsonl"):
        raise ValueError(f"Unexpected classification staging file: {path}")
    body = name[len(CLASSIFICATION_FILENAME_PREFIX) : -len(".jsonl")]
    if "__" in body:
        return body.split("__", 1)[0]
    return body


def _pending_classification_staging_files(
    data_dir: str | Path,
    *,
    batch_id: str | None = None,
    include_open: bool = False,
) -> list[Path]:
    stage_dir = Path(data_dir) / "staging"
    if not stage_dir.exists():
        return []
    prefix = CLASSIFICATION_FILENAME_PREFIX
    stem = f"{prefix}{batch_id}" if batch_id is not None else prefix
    patterns = [f"{stem}*.jsonl"]
    if include_open:
        patterns.append(f"{stem}*.jsonl.open")
    paths: list[Path] = []
    for pattern in patterns:
        paths.extend(stage_dir.glob(pattern))
    deduped = sorted({path.resolve(): path for path in paths}.values())
    return deduped


def _classification_batch_totals(
    con: duckdb.DuckDBPyConnection,
    batch_id: str,
) -> tuple[int, int, int]:
    row = con.execute(
        CLASSIFICATION_BATCH_AGGREGATE_SQL,
        [batch_id],
    ).fetchone()
    if row is None:
        return (0, 0, 0)
    return (int(row[0]), int(row[1]), int(row[2]))


def insert_classification_batch(
    con: duckdb.DuckDBPyConnection,
    *,
    classifier: str,
    source_type: str,
    input_count: int,
    model_version: str,
) -> str:
    batch_id = str(uuid4())
    con.execute(
        """
        INSERT INTO classification_batches (
            batch_id,
            classifier,
            source_type,
            started_at,
            finished_at,
            status,
            input_count,
            classified_count,
            unable_count,
            error_count,
            model_version
        )
        VALUES (?, ?, ?, ?, NULL, 'running', ?, 0, 0, 0, ?)
        """,
        [
            batch_id,
            classifier,
            source_type,
            parse_staged_datetime(isoformat_utc()),
            input_count,
            model_version,
        ],
    )
    return batch_id


def finish_classification_batch(
    con: duckdb.DuckDBPyConnection,
    *,
    batch_id: str,
    status: str,
    classified_count: int | None = None,
    unable_count: int | None = None,
    error_count: int | None = None,
) -> None:
    con.execute(
        """
        UPDATE classification_batches
        SET
            finished_at = CASE
                WHEN ? THEN ?
                ELSE finished_at
            END,
            status = ?,
            classified_count = COALESCE(?, classified_count),
            unable_count = COALESCE(?, unable_count),
            error_count = COALESCE(?, error_count)
        WHERE batch_id = ?
        """,
        [
            status != "running",
            parse_staged_datetime(isoformat_utc()),
            status,
            classified_count,
            unable_count,
            error_count,
            batch_id,
        ],
    )


def _classification_batch_progress(
    con: duckdb.DuckDBPyConnection,
    batch_id: str,
) -> tuple[int, int, int]:
    row = con.execute(
        """
        SELECT
            COALESCE(input_count, 0),
            COALESCE(classified_count, 0),
            COALESCE(error_count, 0)
        FROM classification_batches
        WHERE batch_id = ?
        """,
        [batch_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"Unknown classification batch: {batch_id}")
    return (int(row[0]), int(row[1]), int(row[2]))


def _scan_staged_classification_file(
    path: str | Path,
) -> StagedClassificationFileStats:
    con = duckdb.connect(":memory:")
    try:
        row = con.execute(
            f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (
                    WHERE entity_type != ?
                ) AS invalid_entity_rows,
                COUNT(*) FILTER (
                    WHERE json_type(raw_json) != 'OBJECT'
                ) AS invalid_payload_rows
            FROM {STAGED_JSONL_SCAN_SQL}
            """,
            ["classification", str(path)],
        ).fetchone()
    finally:
        con.close()

    if row is None:
        return StagedClassificationFileStats(0, 0, 0)
    return StagedClassificationFileStats(
        total_rows=int(row[0]),
        invalid_entity_rows=int(row[1]),
        invalid_payload_rows=int(row[2]),
    )


def _load_classification_staging_file(
    db_path: str | Path,
    *,
    path: Path,
    finalize_batch: bool,
) -> LoadedBatch:
    batch_id = _classification_batch_id_from_path(path)

    def ensure_batch_exists(con: duckdb.DuckDBPyConnection) -> None:
        ensure_pipeline_schema(con)
        _classification_batch_progress(con, batch_id)

    with_duckdb_connection(db_path, ensure_batch_exists)

    try:
        staged_stats = _scan_staged_classification_file(path)
        if staged_stats.invalid_entity_rows:
            raise ValueError(
                f"Unexpected entity_type rows in classification staging: {path}"
            )
        if staged_stats.invalid_payload_rows:
            raise ValueError(
                "Expected object payloads in classification staging rows"
            )

        def run_load(con: duckdb.DuckDBPyConnection) -> tuple[int, int, int]:
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute(
                    CLASSIFICATION_INSERT_SQL,
                    [batch_id, str(path)],
                )
                (
                    records_updated,
                    unable_count,
                    error_count,
                ) = _classification_batch_totals(con, batch_id)
                finish_classification_batch(
                    con,
                    batch_id=batch_id,
                    status="succeeded" if finalize_batch else "running",
                    classified_count=records_updated,
                    unable_count=unable_count,
                    error_count=error_count,
                )
                con.execute("COMMIT")
                return (records_updated, unable_count, error_count)
            except Exception:
                con.execute("ROLLBACK")
                raise

        (
            records_updated,
            unable_count,
            error_count,
        ) = with_duckdb_connection(db_path, run_load)
    except Exception:
        def mark_failed(con: duckdb.DuckDBPyConnection) -> None:
            (
                records_updated,
                unable_count,
                error_count,
            ) = _classification_batch_totals(con, batch_id)
            finish_classification_batch(
                con,
                batch_id=batch_id,
                status="failed",
                classified_count=records_updated,
                unable_count=unable_count,
                error_count=error_count + 1,
            )

        with_duckdb_connection(db_path, mark_failed)
        raise

    loaded_path = mark_staging_file_loaded(path)
    return LoadedBatch(
        batch_id=batch_id,
        path=str(loaded_path),
        records_fetched=staged_stats.total_rows,
        records_updated=records_updated,
        error_count=error_count,
        extra={"unable_count": unable_count},
    )


def load_classification_staging(
    data_dir: str | Path,
    db_path: str | Path,
    *,
    batch_id: str | None = None,
    include_open: bool = False,
    finalize_batch: bool = True,
) -> dict[str, object]:
    data_dir = Path(data_dir)
    db_path = Path(db_path)
    loaded: list[LoadedBatch] = []
    loaded_paths: list[str] = []
    touched_batches: set[str] = set()

    for path in _pending_classification_staging_files(
        data_dir,
        batch_id=batch_id,
        include_open=include_open,
    ):
        result = _load_classification_staging_file(
            db_path,
            path=path,
            finalize_batch=finalize_batch,
        )
        loaded.append(result)
        loaded_paths.append(result.path)
        touched_batches.add(result.batch_id)

    batch_totals: list[tuple[str, int, int, int]] = []
    if touched_batches:
        def read_totals(
            con: duckdb.DuckDBPyConnection,
        ) -> list[tuple[str, int, int, int]]:
            totals: list[tuple[str, int, int, int]] = []
            for touched_batch_id in sorted(touched_batches):
                records_updated, unable_count, error_count = (
                    _classification_batch_totals(con, touched_batch_id)
                )
                totals.append(
                    (
                        touched_batch_id,
                        records_updated,
                        unable_count,
                        error_count,
                    )
                )
            return totals

        batch_totals = with_duckdb_connection(db_path, read_totals, read_only=True)

    return {
        "sync_type": CLASSIFICATION_SYNC_TYPE,
        "loaded_batches": len(touched_batches),
        "records_fetched": sum(result.records_fetched for result in loaded),
        "records_updated": sum(total[1] for total in batch_totals),
        "unable_count": sum(total[2] for total in batch_totals),
        "error_count": sum(total[3] for total in batch_totals),
        "batch_ids": sorted(touched_batches),
        "loaded_paths": loaded_paths,
    }


class WebsiteClassifier:
    def __init__(
        self,
        data_dir: str | Path | None = None,
        db_path: str | Path | None = None,
    ) -> None:
        self.data_dir = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
        self.db_path = Path(db_path) if db_path is not None else DEFAULT_DB_PATH
        settings = load_settings(self.data_dir)
        self.llm_config = dict(settings.get("llm", {}))
        self.model = str(self.llm_config.get("model") or "unknown-model")
        self.classifier_name = f"llm:{self.model}"
        self.llm_endpoint = str(
            self.llm_config.get("endpoint") or "http://localhost:9741/v1"
        ).rstrip("/")
        self.classifier_workers = self._classifier_workers()
        self._http_session = requests.Session()
        self._http_session.headers.update({"User-Agent": USER_AGENT})
        adapter = HTTPAdapter(
            max_retries=0,
            pool_connections=10,
            pool_maxsize=10,
        )
        self._http_session.mount("http://", adapter)
        self._http_session.mount("https://", adapter)
        self._llm_client = httpx.Client(
            base_url=f"{self.llm_endpoint}/",
            timeout=60.0,
            http2=True,
        )
        self.playwright_enabled = self._playwright_enabled()

    def __enter__(self) -> "WebsiteClassifier":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def close(self) -> None:
        llm_client = getattr(self, "_llm_client", None)
        if llm_client is not None:
            llm_client.close()
            self._llm_client = None
        http_session = getattr(self, "_http_session", None)
        if http_session is not None:
            http_session.close()
            self._http_session = None

    def _require_http_session(self) -> requests.Session:
        if self._http_session is None:
            raise RuntimeError("WebsiteClassifier is closed")
        return self._http_session

    def _require_llm_client(self) -> httpx.Client:
        if self._llm_client is None:
            raise RuntimeError("WebsiteClassifier is closed")
        return self._llm_client

    def _playwright_enabled(self) -> bool:
        configured = self.llm_config.get("playwright_fallback")
        if isinstance(configured, bool):
            return configured
        return browser.is_playwright_available()

    def _classifier_workers(self) -> int:
        configured = self.llm_config.get("classifier_workers")
        try:
            value = int(configured)
        except (TypeError, ValueError):
            return 3
        return max(1, value)

    def _normalized_company_urls(
        self,
        rows: list[tuple[str, bool]],
    ) -> list[str]:
        normalized = [_normalize_url(url) for url, _ in rows if url.strip()]
        if any(bool(row[1]) for row in rows):
            return normalized[:1]
        deduped: list[str] = []
        for value in normalized:
            if value and value not in deduped:
                deduped.append(value)
        return deduped

    def _select_company_inputs(
        self,
        con: duckdb.DuckDBPyConnection,
        *,
        mode: Mode,
        ids: list[str] | None,
    ) -> list[tuple[str, list[str]]]:
        if mode == "list":
            if not ids:
                raise ValueError("mode=list requires one or more ids")
            placeholders = ", ".join(["?"] * len(ids))
            rows = con.execute(
                f"""
                SELECT company_number, url, is_primary
                FROM company_websites
                WHERE company_number IN ({placeholders})
                  AND url IS NOT NULL
                  AND trim(url) != ''
                ORDER BY company_number, is_primary DESC, discovered_at ASC, website_id ASC
                """,
                ids,
            ).fetchall()
        else:
            joins = ""
            filters = ""
            params: list[object] = []
            if mode == "incremental":
                joins = """
                LEFT JOIN classifications cls
                  ON cls.company_number = m.company_number
                 AND cls.source_type = 'website'
                 AND coalesce(lower(trim(cls.verdict_reason)), '') != 'parse_error'
                """
                filters = """
                  AND cls.company_number IS NULL
                """
            rows = con.execute(
                f"""
                SELECT m.company_number, cw.url, cw.is_primary
                FROM current_company_match m
                INNER JOIN company_websites cw
                  ON cw.company_number = m.company_number
                 AND cw.is_primary = TRUE
                {joins}
                WHERE cw.url IS NOT NULL
                  AND trim(cw.url) != ''
                {filters}
                ORDER BY m.company_number, cw.is_primary DESC, cw.discovered_at ASC, cw.website_id ASC
                """,
                params,
            ).fetchall()

        grouped: list[tuple[str, list[str]]] = []
        current_company: str | None = None
        current_rows: list[tuple[str, bool]] = []
        for company_number, url, is_primary in rows:
            company = str(company_number)
            if current_company is None:
                current_company = company
            if company != current_company:
                urls = self._normalized_company_urls(current_rows)
                if urls:
                    grouped.append((current_company, urls))
                current_company = company
                current_rows = []
            current_rows.append((str(url), bool(is_primary)))
        if current_company is not None:
            urls = self._normalized_company_urls(current_rows)
            if urls:
                grouped.append((current_company, urls))
        return grouped

    def _fetch_page(self, url: str, *, timeout_seconds: float = 15.0) -> PageFetch:
        last_error: Exception | None = None
        last_error_kind: str | None = None
        for candidate_url in [url, _strip_https_www(url)]:
            if candidate_url is None:
                continue
            try:
                response = self._require_http_session().get(
                    candidate_url,
                    timeout=timeout_seconds,
                )
                html = response.text if response.ok else None
                markdown = _extract_markdown(html)
                return PageFetch(
                    path=urlsplit(url).path.strip("/"),
                    url=candidate_url,
                    status=int(response.status_code),
                    html=html,
                    markdown=markdown,
                )
            except requests.exceptions.Timeout as exc:
                last_error = exc
                last_error_kind = "timeout"
                if candidate_url == url and _strip_https_www(url) is not None:
                    continue
                break
            except requests.exceptions.SSLError as exc:
                last_error = exc
                last_error_kind = "ssl_error"
                if candidate_url == url and _strip_https_www(url) is not None:
                    continue
                break
            except requests.exceptions.ConnectionError as exc:
                last_error = exc
                last_error_kind = "connection_error"
                if candidate_url == url and _strip_https_www(url) is not None:
                    continue
                break
        if last_error is not None:
            logger.debug("Page fetch failed for %s: %s", url, last_error)
        return PageFetch(
            path=urlsplit(url).path.strip("/"),
            url=url,
            status=None,
            html=None,
            markdown=None,
            fetch_error=last_error_kind,
        )

    def _page_needs_playwright(self, page: PageFetch) -> bool:
        if page.status == 403:
            return True
        if page.status == 200 and page.text_len < MIN_CLASSIFIABLE_TEXT_LEN:
            return True
        return False

    def _assemble_site_content(
        self,
        base_url: str | None,
        fetched_pages: list[PageFetch],
    ) -> SiteContent:
        retry_pages = tuple(
            (page.path, page.url)
            for page in fetched_pages
            if self._page_needs_playwright(page)
        )
        seen_hashes: set[str] = set()
        bundle_parts: list[str] = []
        pages_used: list[PageFetch] = []
        for page in fetched_pages:
            if page.status is None or not (200 <= page.status < 400):
                continue
            if page.markdown is None or len(page.markdown) < 80:
                continue
            digest = hashlib.sha1(page.markdown[:100].encode("utf-8")).hexdigest()
            if digest in seen_hashes:
                continue
            seen_hashes.add(digest)
            page_header = "/" if not page.path else f"/{page.path}"
            bundle_parts.append(f"## Page: {page_header}\n\n{page.markdown}\n\n---\n\n")
            pages_used.append(page)

        full_content = "".join(bundle_parts)
        truncated = len(full_content) > 12_000
        content = full_content[:12_000]
        any_success = any(
            page.status is not None and 200 <= page.status < 400
            for page in fetched_pages
        )
        failure_reason: str | None = None
        if len(content) < MIN_CLASSIFIABLE_TEXT_LEN:
            failure_reason = "no_content" if any_success else "all_pages_unreachable"

        return SiteContent(
            source_url=base_url,
            pages=pages_used,
            content=content,
            truncated=truncated,
            used_playwright=any(page.used_playwright for page in fetched_pages),
            failure_reason=failure_reason,
            http_status=200 if any_success else None,
            retry_pages=retry_pages,
        )

    def _fetch_site_pages_parallel(self, base_url: str) -> list[PageFetch]:
        page_urls = [_page_url(base_url, path) for path in PAGE_PATHS]
        fetched_pages: list[PageFetch | None] = [None] * len(page_urls)
        with ThreadPoolExecutor(max_workers=len(PAGE_PATHS)) as executor:
            futures = {
                executor.submit(self._fetch_page, page_url): index
                for index, page_url in enumerate(page_urls)
            }
            for future in as_completed(futures):
                index = futures[future]
                page_url = page_urls[index]
                try:
                    fetched_pages[index] = future.result()
                except Exception as exc:
                    logger.exception("Parallel page fetch failed for %s", page_url)
                    fetched_pages[index] = PageFetch(
                        path=urlsplit(page_url).path.strip("/"),
                        url=page_url,
                        status=None,
                        html=None,
                        markdown=None,
                        fetch_error=type(exc).__name__,
                    )
        return [page for page in fetched_pages if page is not None]

    def _collect_site_content(self, base_url: str) -> SiteContent:
        fetched_pages = self._fetch_site_pages_parallel(base_url)
        return self._assemble_site_content(base_url, fetched_pages)

    def _collect_site_content_with_playwright(
        self,
        site: SiteContent,
        session: browser.PlaywrightSession,
    ) -> SiteContent:
        fetched_pages: list[PageFetch] = []
        for path, page_url in site.retry_pages:
            rendered_html = browser.fetch_rendered(
                page_url,
                session=session,
            )
            markdown = _extract_markdown(rendered_html)
            fetched_pages.append(
                PageFetch(
                    path=path,
                    url=page_url,
                    status=200 if rendered_html else None,
                    html=rendered_html,
                    markdown=markdown,
                    used_playwright=rendered_html is not None,
                    fetch_error=None if rendered_html else "playwright_error",
                )
            )
        return self._assemble_site_content(site.source_url, fetched_pages)

    def _call_llm(self, content: str) -> tuple[str | None, dict[str, Any] | None]:
        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "user",
                    "content": PROMPT_TEMPLATE.format(content=content),
                }
            ],
            "max_tokens": int(self.llm_config.get("max_tokens") or 400),
            "temperature": float(self.llm_config.get("temperature") or 0.1),
            "response_format": {"type": "json_object"},
        }
        response = self._require_llm_client().post(
            "chat/completions",
            json=payload,
        )
        response.raise_for_status()
        payload_json = response.json()
        choices = payload_json.get("choices") or []
        if not choices:
            return None, None
        message = (choices[0] or {}).get("message") or {}
        raw_content = _extract_text_content(message)
        parsed = _extract_json_object(raw_content) if raw_content else None
        return raw_content or None, parsed

    def _unable_row(
        self,
        *,
        company_number: str,
        site: SiteContent,
        failure_reason: str,
        raw_response: str | None = None,
        error: str | None = None,
    ) -> StagedClassification:
        return StagedClassification(
            entity_id=company_number,
            entity_type="classification",
            fetched_at=isoformat_utc(),
            http_status=site.http_status,
            raw_json={
                "verdict": "Unable to classify",
                "evidence": "",
                "classifier": self.classifier_name,
                "source_url": site.source_url,
                "pages_used": [
                    {
                        "path": page.path,
                        "len": page.text_len,
                        "status": page.status,
                    }
                    for page in site.pages
                ],
                "truncated": site.truncated,
                "used_playwright": site.used_playwright,
                "failure_reason": failure_reason,
                "raw_response": raw_response,
                "error": error,
            },
        )

    def _classify_site_content(
        self,
        company_number: str,
        site: SiteContent,
    ) -> tuple[StagedClassification, dict[str, object]]:
        if site.text_len < MIN_CLASSIFIABLE_TEXT_LEN:
            staged = self._unable_row(
                company_number=company_number,
                site=site,
                failure_reason=site.failure_reason or "no_content",
            )
            return staged, {
                "n_pages": len(site.pages),
                "text_len": site.text_len,
                "verdict": "Unable to classify",
                "used_playwright": site.used_playwright,
                "llm_latency": 0.0,
            }

        llm_started = time.monotonic()
        try:
            raw_response, parsed = self._call_llm(site.content)
        except Exception as exc:
            logger.exception("LLM classification failed for %s", company_number)
            staged = self._unable_row(
                company_number=company_number,
                site=site,
                failure_reason=LLM_REQUEST_FAILURE_REASON,
                error=str(exc),
            )
            return staged, {
                "n_pages": len(site.pages),
                "text_len": site.text_len,
                "verdict": "Unable to classify",
                "used_playwright": site.used_playwright,
                "llm_latency": time.monotonic() - llm_started,
            }

        llm_latency = time.monotonic() - llm_started
        verdict = _canonical_verdict((parsed or {}).get("verdict"))
        if verdict not in VALID_VERDICTS:
            staged = self._unable_row(
                company_number=company_number,
                site=site,
                failure_reason="parse_error",
                raw_response=raw_response,
            )
            return staged, {
                "n_pages": len(site.pages),
                "text_len": site.text_len,
                "verdict": "Unable to classify",
                "used_playwright": site.used_playwright,
                "llm_latency": llm_latency,
            }

        staged = StagedClassification(
            entity_id=company_number,
            entity_type="classification",
            fetched_at=isoformat_utc(),
            http_status=site.http_status,
            raw_json={
                "verdict": verdict,
                "evidence": str((parsed or {}).get("evidence") or "").strip(),
                "classifier": self.classifier_name,
                "source_url": site.source_url,
                "pages_used": [
                    {
                        "path": page.path,
                        "len": page.text_len,
                        "status": page.status,
                    }
                    for page in site.pages
                ],
                "truncated": site.truncated,
                "used_playwright": site.used_playwright,
                "failure_reason": None,
            },
        )
        return staged, {
            "n_pages": len(site.pages),
            "text_len": site.text_len,
            "verdict": verdict,
            "used_playwright": site.used_playwright,
            "llm_latency": llm_latency,
        }

    def _classify_company(
        self,
        company_number: str,
        urls: list[str],
    ) -> tuple[StagedClassification, dict[str, object]] | FallbackTask:
        fetch_started = time.monotonic()
        chosen_site: SiteContent | None = None
        fallback_site: SiteContent | None = None
        for url in urls:
            site = self._collect_site_content(url)
            if chosen_site is None or site.text_len > chosen_site.text_len:
                chosen_site = site
            if site.text_len >= MIN_CLASSIFIABLE_TEXT_LEN:
                fetch_latency = time.monotonic() - fetch_started
                staged, metrics = self._classify_site_content(company_number, site)
                metrics["fetch_latency"] = fetch_latency
                return staged, metrics
            if self.playwright_enabled and site.retry_pages and fallback_site is None:
                fallback_site = site

        if chosen_site is None:
            chosen_site = SiteContent(
                source_url=None,
                pages=[],
                content="",
                truncated=False,
                used_playwright=False,
                failure_reason="no_content",
                http_status=None,
                retry_pages=(),
            )

        if fallback_site is not None:
            return FallbackTask(
                company_number=company_number,
                site=fallback_site,
                fetch_latency=time.monotonic() - fetch_started,
            )
        fetch_latency = time.monotonic() - fetch_started
        staged, metrics = self._classify_site_content(company_number, chosen_site)
        metrics["fetch_latency"] = fetch_latency
        return staged, metrics

    def _classify_company_with_playwright(
        self,
        task: FallbackTask,
        session: browser.PlaywrightSession,
    ) -> tuple[StagedClassification, dict[str, object]]:
        fetch_started = time.monotonic()
        site = self._collect_site_content_with_playwright(task.site, session)
        fetch_latency = task.fetch_latency + (time.monotonic() - fetch_started)
        staged, metrics = self._classify_site_content(task.company_number, site)
        metrics["fetch_latency"] = fetch_latency
        return staged, metrics

    def classify(
        self,
        *,
        mode: str = "incremental",
        ids: list[str] | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> dict[str, object]:
        validated_mode = _validated_mode(mode)
        validated_batch_size = _validated_batch_size(batch_size)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        batch_id: str | None = None
        run_log: FsyncLineLogger | None = None
        main_writer: ClassificationChunkWriter | None = None
        fallback_writer: ClassificationChunkWriter | None = None
        fallback_thread: threading.Thread | None = None
        main_threads: list[threading.Thread] = []
        company_inputs: list[tuple[str, list[str]]] = []
        dispatched = 0
        fallback_enqueued = 0
        started_monotonic = time.monotonic()
        log_lock = threading.Lock()
        state_lock = threading.Lock()
        main_writer_lock = threading.Lock()
        fallback_writer_lock = threading.Lock()
        stop_event = threading.Event()
        load_requested = threading.Event()
        main_input_queue: queue.Queue[tuple[str, list[str]] | None] = queue.Queue()
        fallback_queue: queue.Queue[FallbackTask | None] = queue.Queue()
        main_failures: list[BaseException] = []
        fallback_failures: list[BaseException] = []
        try:
            load_classification_staging(
                self.data_dir,
                self.db_path,
            )

            def prepare_batch(
                con: duckdb.DuckDBPyConnection,
            ) -> tuple[list[tuple[str, list[str]]], str]:
                ensure_pipeline_schema(con)
                selected = self._select_company_inputs(
                    con,
                    mode=validated_mode,
                    ids=ids,
                )
                return (
                    selected,
                    insert_classification_batch(
                        con,
                        classifier=self.classifier_name,
                        source_type="website",
                        input_count=len(selected),
                        model_version=self.model,
                    ),
                )

            company_inputs, batch_id = with_duckdb_connection(
                self.db_path,
                prepare_batch,
            )
            total_requested = len(company_inputs)
            worker_count = min(self.classifier_workers, total_requested) if total_requested else 0

            def log_line(message: str) -> None:
                if run_log is None:
                    return
                with log_lock:
                    run_log.write_line(message)

            def flush_log() -> None:
                if run_log is None:
                    return
                with log_lock:
                    run_log.flush_and_fsync()

            def load_ready_chunks() -> dict[str, object]:
                load_requested.clear()
                if batch_id is None:
                    return {
                        "records_fetched": 0,
                        "records_updated": 0,
                        "unable_count": 0,
                        "error_count": 0,
                        "loaded_paths": [],
                    }
                ready_paths = _pending_classification_staging_files(
                    self.data_dir,
                    batch_id=batch_id,
                )
                if not ready_paths:
                    return {
                        "records_fetched": 0,
                        "records_updated": 0,
                        "unable_count": 0,
                        "error_count": 0,
                        "loaded_paths": [],
                    }
                log_line(f"load_start files={len(ready_paths)}")
                flush_log()
                summary = load_classification_staging(
                    self.data_dir,
                    self.db_path,
                    batch_id=batch_id,
                    finalize_batch=False,
                )
                log_line(
                    "load_done "
                    f"files={len(summary['loaded_paths'])} "
                    f"classified={summary['records_updated']} "
                    f"unable={summary['unable_count']} "
                    f"errors={summary['error_count']}"
                )
                flush_log()
                return summary

            def finalize_writer(
                writer: ClassificationChunkWriter | None,
                *,
                lane: str,
                lock: threading.Lock | None = None,
            ) -> None:
                if writer is None:
                    return
                if lock is None:
                    ready_path = writer.finalize()
                else:
                    with lock:
                        ready_path = writer.finalize()
                if ready_path is not None:
                    log_line(f"chunk lane={lane} path={ready_path.name}")
                    load_requested.set()

            def batch_totals(
                *,
                status: str | None = None,
                bump_error: bool = False,
            ) -> tuple[int, int, int]:
                if batch_id is None:
                    return (0, 0, 0)

                def read_totals(con: duckdb.DuckDBPyConnection) -> tuple[int, int, int]:
                    ensure_pipeline_schema(con)
                    records_updated, unable_count, error_count = (
                        _classification_batch_totals(con, batch_id)
                    )
                    if status is not None:
                        finish_classification_batch(
                            con,
                            batch_id=batch_id,
                            status=status,
                            classified_count=records_updated,
                            unable_count=unable_count,
                            error_count=error_count + (1 if bump_error else 0),
                        )
                    return (
                        records_updated,
                        unable_count,
                        error_count + (1 if bump_error else 0),
                    )

                return with_duckdb_connection(self.db_path, read_totals)

            def raise_main_failure() -> None:
                if main_failures:
                    raise main_failures[0]

            def raise_fallback_failure() -> None:
                if fallback_failures:
                    raise fallback_failures[0]

            run_log = FsyncLineLogger(
                self.data_dir,
                sync_type=CLASSIFICATION_SYNC_TYPE,
                batch_id=batch_id,
                filename_prefix="classify",
            )
            main_writer = ClassificationChunkWriter(
                self.data_dir,
                batch_id=batch_id,
                lane="main",
            )
            fallback_writer = ClassificationChunkWriter(
                self.data_dir,
                batch_id=batch_id,
                lane="fallback",
            )
            log_line(
                f"start mode={validated_mode} requested={total_requested} "
                f"batch_size={validated_batch_size} workers={worker_count} model={self.model}"
            )
            flush_log()

            if not company_inputs:
                def finish_empty_batch(con: duckdb.DuckDBPyConnection) -> None:
                    finish_classification_batch(
                        con,
                        batch_id=batch_id,
                        status="succeeded",
                        classified_count=0,
                        unable_count=0,
                        error_count=0,
                    )

                with_duckdb_connection(self.db_path, finish_empty_batch)
                log_line(
                    "complete requested=0 classified=0 unable=0 errors=0 elapsed=0s"
                )
                flush_log()
                return {
                    "batch_id": batch_id,
                    "requested": 0,
                    "records_fetched": 0,
                    "records_updated": 0,
                    "unable_count": 0,
                    "error_count": 0,
                    "mode": validated_mode,
                    "log_path": str(run_log.path),
                    "playwright_enabled": self.playwright_enabled,
                }

            if self.playwright_enabled:
                def fallback_worker() -> None:
                    assert fallback_writer is not None
                    try:
                        with browser.PlaywrightSession() as session:
                            while True:
                                task = fallback_queue.get()
                                if task is None:
                                    fallback_queue.task_done()
                                    break
                                started_company = time.monotonic()
                                try:
                                    staged_row, metrics = self._classify_company_with_playwright(
                                        task,
                                        session,
                                    )
                                except Exception as exc:
                                    logger.exception(
                                        "Playwright fallback failed for %s",
                                        task.company_number,
                                    )
                                    fallback_site = SiteContent(
                                        source_url=task.site.source_url,
                                        pages=[],
                                        content="",
                                        truncated=False,
                                        used_playwright=True,
                                        failure_reason="no_content",
                                        http_status=None,
                                        retry_pages=(),
                                    )
                                    staged_row = self._unable_row(
                                        company_number=task.company_number,
                                        site=fallback_site,
                                        failure_reason="parse_error",
                                        error=str(exc),
                                    )
                                    metrics = {
                                        "n_pages": 0,
                                        "text_len": 0,
                                        "verdict": "Unable to classify",
                                        "used_playwright": True,
                                        "fetch_latency": task.fetch_latency,
                                        "llm_latency": 0.0,
                                    }
                                with fallback_writer_lock:
                                    fallback_writer.append(staged_row)
                                    ready_path = None
                                    if fallback_writer.rows_in_file >= validated_batch_size:
                                        ready_path = fallback_writer.finalize()
                                log_line(
                                    "fallback "
                                    f"company_number={task.company_number} "
                                    f"n_pages={metrics['n_pages']} "
                                    f"text_len={metrics['text_len']} "
                                    f"verdict={metrics['verdict']} "
                                    f"fetch_latency={metrics['fetch_latency']:.2f}s "
                                    f"llm_latency={metrics['llm_latency']:.2f}s "
                                    f"latency={time.monotonic() - started_company:.2f}s "
                                    f"used_playwright={str(metrics['used_playwright']).lower()}"
                                )
                                if ready_path is not None:
                                    log_line(f"chunk lane=fallback path={ready_path.name}")
                                    load_requested.set()
                                fallback_queue.task_done()
                    except BaseException as exc:
                        fallback_failures.append(exc)
                        stop_event.set()
                    finally:
                        while True:
                            try:
                                item = fallback_queue.get_nowait()
                            except queue.Empty:
                                break
                            fallback_queue.task_done()
                        finalize_writer(
                            fallback_writer,
                            lane="fallback",
                            lock=fallback_writer_lock,
                        )

                fallback_thread = threading.Thread(
                    target=fallback_worker,
                    name=f"classifier-fallback-{batch_id}",
                    daemon=True,
                )
                fallback_thread.start()

            def main_worker() -> None:
                nonlocal dispatched, fallback_enqueued
                assert main_writer is not None
                try:
                    while not stop_event.is_set():
                        item = main_input_queue.get()
                        if item is None or stop_event.is_set():
                            break
                        company_number, urls = item
                        started_company = time.monotonic()
                        try:
                            result = self._classify_company(company_number, urls)
                        except Exception as exc:
                            logger.exception("Classifier failed for %s", company_number)
                            error_site = SiteContent(
                                source_url=urls[0] if urls else None,
                                pages=[],
                                content="",
                                truncated=False,
                                used_playwright=False,
                                failure_reason="parse_error",
                                http_status=None,
                                retry_pages=(),
                            )
                            staged_row = self._unable_row(
                                company_number=company_number,
                                site=error_site,
                                failure_reason="parse_error",
                                error=str(exc),
                            )
                            metrics = {
                                "n_pages": 0,
                                "text_len": 0,
                                "verdict": "Unable to classify",
                                "used_playwright": False,
                                "fetch_latency": 0.0,
                                "llm_latency": 0.0,
                            }
                            result = (staged_row, metrics)

                        if isinstance(result, FallbackTask):
                            fallback_queue.put(result)
                            with state_lock:
                                dispatched += 1
                                fallback_enqueued += 1
                                completed_count = dispatched
                                progress = _progress_text(dispatched, total_requested)
                            log_line(
                                "handoff "
                                f"company_number={company_number} "
                                f"reason={result.site.failure_reason or 'playwright_candidate'} "
                                f"fetch_latency={result.fetch_latency:.2f}s "
                                f"latency={time.monotonic() - started_company:.2f}s "
                                f"progress={progress}"
                            )
                        else:
                            staged_row, metrics = result
                            ready_path: Path | None = None
                            with main_writer_lock:
                                main_writer.append(staged_row)
                                if main_writer.rows_in_file >= validated_batch_size:
                                    ready_path = main_writer.finalize()
                            with state_lock:
                                dispatched += 1
                                completed_count = dispatched
                                progress = _progress_text(dispatched, total_requested)
                            log_line(
                                f"company_number={company_number} "
                                f"n_pages={metrics['n_pages']} "
                                f"text_len={metrics['text_len']} "
                                f"verdict={metrics['verdict']} "
                                f"fetch_latency={metrics['fetch_latency']:.2f}s "
                                f"llm_latency={metrics['llm_latency']:.2f}s "
                                f"latency={time.monotonic() - started_company:.2f}s "
                                f"used_playwright={str(metrics['used_playwright']).lower()} "
                                f"progress={progress}"
                            )
                            if ready_path is not None:
                                log_line(f"chunk lane=main path={ready_path.name}")
                                load_requested.set()
                        if completed_count % validated_batch_size == 0:
                            flush_log()
                except BaseException as exc:
                    main_failures.append(exc)
                    stop_event.set()

            for company_input in company_inputs:
                main_input_queue.put(company_input)

            for index in range(worker_count):
                worker = threading.Thread(
                    target=main_worker,
                    name=f"classifier-main-{batch_id}-{index + 1}",
                    daemon=True,
                )
                main_threads.append(worker)
                worker.start()

            for _ in main_threads:
                main_input_queue.put(None)

            while any(thread.is_alive() for thread in main_threads):
                raise_main_failure()
                raise_fallback_failure()
                if load_requested.wait(timeout=0.25):
                    load_ready_chunks()

            for thread in main_threads:
                thread.join()
            raise_main_failure()
            raise_fallback_failure()

            finalize_writer(
                main_writer,
                lane="main",
                lock=main_writer_lock,
            )
            flush_log()
            load_ready_chunks()

            if fallback_thread is not None:
                fallback_queue.put(None)
                while fallback_thread.is_alive():
                    raise_main_failure()
                    raise_fallback_failure()
                    if load_requested.wait(timeout=0.25):
                        load_ready_chunks()
                fallback_thread.join()
                raise_fallback_failure()

            flush_log()
            load_ready_chunks()
            records_updated, unable_count, error_count = batch_totals(status="succeeded")
            elapsed_seconds = time.monotonic() - started_monotonic
            log_line(
                "complete "
                f"requested={total_requested} "
                f"classified={records_updated} "
                f"unable={unable_count} "
                f"errors={error_count} "
                f"fallback_enqueued={fallback_enqueued} "
                f"elapsed={_duration_text(elapsed_seconds)}"
            )
            flush_log()
            return {
                "batch_id": batch_id,
                "requested": total_requested,
                "records_fetched": total_requested,
                "records_updated": records_updated,
                "unable_count": unable_count,
                "error_count": error_count,
                "mode": validated_mode,
                "log_path": str(run_log.path),
                "playwright_enabled": self.playwright_enabled,
            }
        except BaseException:
            stop_event.set()
            for _ in main_threads:
                main_input_queue.put(None)
            if fallback_thread is not None and fallback_thread.is_alive():
                fallback_queue.put(None)
                fallback_thread.join(timeout=5.0)
            for thread in main_threads:
                thread.join(timeout=5.0)
            if main_writer is not None:
                with main_writer_lock:
                    main_writer.finalize()
            if fallback_writer is not None:
                with fallback_writer_lock:
                    fallback_writer.finalize()
            if batch_id is not None:
                def mark_failed(con: duckdb.DuckDBPyConnection) -> None:
                    ensure_pipeline_schema(con)
                    records_updated, unable_count, error_count = (
                        _classification_batch_totals(con, batch_id)
                    )
                    finish_classification_batch(
                        con,
                        batch_id=batch_id,
                        status="failed",
                        classified_count=records_updated,
                        unable_count=unable_count,
                        error_count=error_count + 1,
                    )

                with_duckdb_connection(self.db_path, mark_failed)
            if run_log is not None:
                log_line(
                    "crash "
                    f"dispatched={dispatched} "
                    f"fallback_enqueued={fallback_enqueued}"
                )
                flush_log()
            raise
        finally:
            if run_log is not None:
                run_log.close()
            if main_writer is not None:
                main_writer.close()
            if fallback_writer is not None:
                fallback_writer.close()
