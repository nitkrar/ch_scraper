"""Website classification orchestration."""

from __future__ import annotations

import logging
import queue
import threading
import time
from pathlib import Path
from typing import Any, Literal

import duckdb
import httpx
import requests
from requests.adapters import HTTPAdapter

from ch_bulk.web import browser
from ch_bulk.core.logging import FsyncLineLogger
from ch_bulk.core.paths import DEFAULT_DATA_DIR, default_db_path
from ch_bulk.core.settings import load_settings
from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.db.staging import with_duckdb_connection
from ch_bulk.web.classifier_content import (
    MIN_CLASSIFIABLE_TEXT_LEN,
    FallbackTask,
    SiteContent,
    _collect_site_content,
    _collect_site_content_with_playwright,
    _normalize_url,
)
from ch_bulk.web.classifier_llm import (
    _classify_site_content,
    _unable_row,
)
from ch_bulk.web.classifier_staging import (
    CLASSIFICATION_SYNC_TYPE,
    ClassificationChunkWriter,
    StagedClassification,
    _classification_batch_totals,
    _pending_classification_staging_files,
    finish_classification_batch,
    insert_classification_batch,
    load_classification_staging,
)

logger = logging.getLogger(__name__)

Mode = Literal["incremental", "all", "list"]
DEFAULT_BATCH_SIZE = 100
USER_AGENT = "Mozilla/5.0 UK-Homecare-Toolkit"


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


class WebsiteClassifier:
    def __init__(
        self,
        data_dir: str | Path | None = None,
        db_path: str | Path | None = None,
    ) -> None:
        self.data_dir = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
        self.db_path = Path(db_path) if db_path is not None else default_db_path(self.data_dir)
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

    def _classify_company(
        self,
        company_number: str,
        urls: list[str],
    ) -> tuple[StagedClassification, dict[str, object]] | FallbackTask:
        fetch_started = time.monotonic()
        chosen_site: SiteContent | None = None
        fallback_site: SiteContent | None = None
        for url in urls:
            site = _collect_site_content(self._require_http_session(), url)
            if chosen_site is None or site.text_len > chosen_site.text_len:
                chosen_site = site
            if site.text_len >= MIN_CLASSIFIABLE_TEXT_LEN:
                fetch_latency = time.monotonic() - fetch_started
                staged, metrics = _classify_site_content(
                    company_number=company_number,
                    site=site,
                    llm_client=self._require_llm_client(),
                    model=self.model,
                    llm_config=self.llm_config,
                    classifier_name=self.classifier_name,
                )
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
        staged, metrics = _classify_site_content(
            company_number=company_number,
            site=chosen_site,
            llm_client=self._require_llm_client(),
            model=self.model,
            llm_config=self.llm_config,
            classifier_name=self.classifier_name,
        )
        metrics["fetch_latency"] = fetch_latency
        return staged, metrics

    def _classify_company_with_playwright(
        self,
        task: FallbackTask,
        session: browser.PlaywrightSession,
    ) -> tuple[StagedClassification, dict[str, object]]:
        fetch_started = time.monotonic()
        site = _collect_site_content_with_playwright(task.site, session)
        fetch_latency = task.fetch_latency + (time.monotonic() - fetch_started)
        staged, metrics = _classify_site_content(
            company_number=task.company_number,
            site=site,
            llm_client=self._require_llm_client(),
            model=self.model,
            llm_config=self.llm_config,
            classifier_name=self.classifier_name,
        )
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
            worker_count = (
                min(self.classifier_workers, total_requested) if total_requested else 0
            )

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
                                    staged_row = _unable_row(
                                        company_number=task.company_number,
                                        site=fallback_site,
                                        classifier_name=self.classifier_name,
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
                            staged_row = _unable_row(
                                company_number=company_number,
                                site=error_site,
                                classifier_name=self.classifier_name,
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
