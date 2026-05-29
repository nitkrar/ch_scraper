"""Pipeline orchestration for financial filing enrichment."""

from __future__ import annotations

import logging
import multiprocessing as mp
import queue
import signal
import threading
import time
from multiprocessing.connection import Connection, wait as wait_for_connections
from pathlib import Path
from typing import Any

import duckdb

from ch_bulk.companies_house.financials_contracts import (
    ERROR_PARSE_STATUSES,
    FINANCIALS_FETCH_SYNC_TYPE,
    FINANCIALS_SYNC_TYPE,
    Mode,
    FinancialTarget,
    FinancialsResult,
    FetchedFinancialRow,
    FetchedFinancialWorkItem,
    ParserProcessHandle,
    ParserProcessResponse,
)
from ch_bulk.core.cancellation import OperationCancelled
from ch_bulk.companies_house.financials_fetch import (
    CH_WINDOW_SECONDS,
    EFFECTIVE_CH_MAX_REQUESTS,
    CompaniesHouseFinancialsClient,
    _fetch_company_work_item,
    _select_targets,
)
from ch_bulk.companies_house.financials_staging import (
    _archive_financials_fetch_manifest,
    _mark_stale_running_batches,
    _parse_fetched_row,
    load_financials_staging,
    replay_financials_fetch_staging,
)
from ch_bulk.core.logging import FsyncLineLogger
from ch_bulk.core.paths import DEFAULT_DATA_DIR
from ch_bulk.core.rate_limit import SlidingWindowThrottle
from ch_bulk.core.settings import load_settings
from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.db.staging import StagingWriter, with_duckdb_connection
from ch_bulk.db.sync_batches import (
    finish_sync_batch,
    insert_sync_batch,
    update_sync_batch_progress,
)

logger = logging.getLogger(__name__)

DEFAULT_WORKERS = 3
DEFAULT_PARSER_WORKERS = 4
DEFAULT_BATCH_SIZE = 100
DEFAULT_QUEUE_MAXSIZE = 100
DEFAULT_PARSE_INFLIGHT_PER_WORKER = 2
PIPELINE_HEARTBEAT_INTERVAL_SECONDS = 30.0


def _validated_mode(mode: str) -> Mode:
    normalized = mode.strip().lower()
    if normalized not in {"incremental", "all", "list"}:
        raise ValueError(f"Unsupported mode: {mode}")
    return normalized  # type: ignore[return-value]


def _validated_batch_size(batch_size: int) -> int:
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    return batch_size


def _validated_workers(workers: int) -> int:
    if workers <= 0:
        raise ValueError("workers must be > 0")
    return workers


def _validated_parser_workers(parser_workers: int) -> int:
    if parser_workers <= 0:
        raise ValueError("parser_workers must be > 0")
    return parser_workers


def _progress_text(done: int, total: int) -> str:
    if total <= 0:
        return "0/0 0.0%"
    return f"{done}/{total} {(done / total) * 100:.1f}%"


def _duration_text(seconds: float) -> str:
    rounded = max(0, int(round(seconds)))
    hours, remainder = divmod(rounded, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def _parser_process_main(connection: Connection) -> None:
    try:
        while True:
            row = connection.recv()
            if row is None:
                break
            try:
                parsed_row = _parse_fetched_row(row)
            except BaseException as exc:
                connection.send(
                    ParserProcessResponse(
                        row=None,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )
            else:
                connection.send(
                    ParserProcessResponse(
                        row=parsed_row,
                        error=None,
                    )
                )
    finally:
        connection.close()


def enrich_financials(
    db_path: str | Path,
    data_dir: str | Path | None = None,
    *,
    mode: str = "incremental",
    ids: list[str] | None = None,
    workers: int = DEFAULT_WORKERS,
    parser_workers: int = DEFAULT_PARSER_WORKERS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    cancel_event: threading.Event | None = None,
) -> dict[str, object]:
    validated_mode = _validated_mode(mode)
    validated_workers = _validated_workers(workers)
    validated_parser_workers = _validated_parser_workers(parser_workers)
    validated_batch_size = _validated_batch_size(batch_size)
    db_path = Path(db_path)
    data_dir = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
    settings = load_settings(data_dir)
    api_key = str(settings["api_keys"]["companies_house"])
    if not api_key:
        raise RuntimeError("Companies House API key is not configured in settings.json")

    batch_id: str | None = None
    run_log: FsyncLineLogger | None = None
    staging_writer: StagingWriter | None = None
    fetch_staging_writer: StagingWriter | None = None
    parser_processes: list[ParserProcessHandle] = []
    started_monotonic = time.monotonic()
    targets: list[FinancialTarget] = []
    total_errors = 0
    processed = 0
    staged_since_sync = 0
    replayed_fetch_rows = 0
    fetched_count = 0
    pdf_no_text_layer_count = 0
    shutdown_logged = False
    worker_failures: list[BaseException] = []
    result_queue: queue.Queue[FinancialsResult | None] | None = None
    input_queue: queue.Queue[FinancialTarget | None] | None = None
    fetched_queue: queue.Queue[FetchedFinancialWorkItem | None] | None = None
    fetch_manifest_lock = threading.Lock()
    counters_lock = threading.Lock()
    fetch_workers_remaining = 0
    fetch_workers_remaining_lock = threading.Lock()
    parser_controller_finished = threading.Event()
    shutdown_event = threading.Event()
    shutdown_reason: dict[str, str | None] = {"value": None}
    original_signal_handlers: dict[int, Any] = {}
    last_pipeline_heartbeat_monotonic = started_monotonic
    queue_maxsize = 0
    parser_worker_count = 0
    parser_future_limit = 0
    cancellation_finalized = False

    replayed_fetch_rows = replay_financials_fetch_staging(data_dir)
    load_financials_staging(data_dir, db_path)

    def prepare_batch(
        con: duckdb.DuckDBPyConnection,
    ) -> tuple[list[FinancialTarget], str]:
        ensure_pipeline_schema(con)
        _mark_stale_running_batches(con)
        selected = _select_targets(
            con,
            mode=validated_mode,
            ids=ids,
            data_dir=data_dir,
        )
        batch = insert_sync_batch(
            con,
            sync_type=FINANCIALS_SYNC_TYPE,
            mode=validated_mode,
        )
        return selected, batch

    targets, batch_id = with_duckdb_connection(db_path, prepare_batch)
    total_requested = len(targets)
    worker_count = min(validated_workers, total_requested) if total_requested else 0
    parser_worker_count = (
        min(validated_parser_workers, total_requested) if total_requested else 0
    )
    parser_future_limit = parser_worker_count
    queue_maxsize = max(
        10,
        min(
            DEFAULT_QUEUE_MAXSIZE,
            max(validated_batch_size, parser_future_limit),
        ),
    )
    input_queue = queue.Queue(maxsize=queue_maxsize)
    fetched_queue = queue.Queue(maxsize=queue_maxsize)
    result_queue = queue.Queue(maxsize=queue_maxsize)

    run_log = FsyncLineLogger(
        data_dir,
        sync_type=FINANCIALS_SYNC_TYPE,
        batch_id=batch_id,
        filename_prefix="financials",
    )
    staging_writer = StagingWriter(
        data_dir,
        sync_type=FINANCIALS_SYNC_TYPE,
        batch_id=batch_id,
    )
    fetch_staging_writer = StagingWriter(
        data_dir,
        sync_type=FINANCIALS_FETCH_SYNC_TYPE,
        batch_id=batch_id,
    )
    run_log.write_line(
        "start "
        f"sync_type={FINANCIALS_SYNC_TYPE} "
        f"requested={total_requested} "
        f"mode={validated_mode} "
        f"fetch_workers={worker_count} "
        f"parser_workers={parser_worker_count} "
        f"queue_maxsize={queue_maxsize} "
        f"parser_inflight_limit={parser_future_limit} "
        f"batch_size={validated_batch_size} "
        f"replayed_fetch_rows={replayed_fetch_rows}"
    )
    run_log.flush_and_fsync()

    if not targets:
        def finish_empty(con: duckdb.DuckDBPyConnection) -> None:
            ensure_pipeline_schema(con)
            finish_sync_batch(
                con,
                batch_id,
                status="succeeded",
                records_fetched=0,
                records_updated=0,
                error_count=0,
            )

        with_duckdb_connection(db_path, finish_empty)
        return {
            "batch_id": batch_id,
            "requested": 0,
            "records_fetched": 0,
            "records_updated": 0,
            "ok_count": 0,
            "partial_count": 0,
            "ixbrl_count": 0,
            "pdf_count": 0,
            "pdf_no_text_layer_count": 0,
            "no_filing_count": 0,
            "stale_count": 0,
            "error_count": 0,
            "log_path": str(run_log.path),
            "mode": validated_mode,
        }

    throttle = SlidingWindowThrottle(
        EFFECTIVE_CH_MAX_REQUESTS,
        CH_WINDOW_SECONDS,
        label="companies_house_financials",
        logger=logger,
    )

    def queue_depth(q: queue.Queue[Any] | None) -> int:
        if q is None:
            return 0
        try:
            return q.qsize()
        except NotImplementedError:
            return -1

    def request_shutdown(reason: str) -> None:
        shutdown_event.set()
        if shutdown_reason["value"] is None:
            shutdown_reason["value"] = reason

    def note_external_cancel() -> None:
        if cancel_event is not None and cancel_event.is_set():
            request_shutdown("cancelled")

    def note_shutdown_if_needed() -> None:
        nonlocal shutdown_logged
        if shutdown_event.is_set() and not shutdown_logged and run_log is not None:
            run_log.write_line(
                f"shutdown_requested reason={shutdown_reason['value'] or 'external'}"
            )
            run_log.flush_and_fsync()
            shutdown_logged = True

    def persist_batch_progress() -> None:
        if batch_id is None:
            return

        def update_progress(con: duckdb.DuckDBPyConnection) -> None:
            ensure_pipeline_schema(con)
            update_sync_batch_progress(
                con,
                batch_id,
                records_fetched=fetched_count,
                records_updated=processed,
                error_count=total_errors,
            )

        with_duckdb_connection(db_path, update_progress)

    def checkpoint_write_phase() -> None:
        nonlocal staged_since_sync
        if run_log is None or staging_writer is None or fetch_staging_writer is None:
            return
        with fetch_manifest_lock:
            fetch_staging_writer.flush_and_fsync()
        if staged_since_sync > 0:
            staging_writer.flush_and_fsync()
            elapsed = time.monotonic() - started_monotonic
            eta_seconds = (
                ((elapsed / processed) * max(total_requested - processed, 0))
                if processed
                else 0.0
            )
            run_log.write_line(
                "flush "
                f"batch_size={staged_since_sync} "
                f"processed={processed} "
                f"errors={total_errors} "
                f"elapsed={_duration_text(elapsed)} "
                f"eta={_duration_text(eta_seconds)}"
            )
            staged_since_sync = 0
        run_log.flush_and_fsync()
        persist_batch_progress()

    def maybe_log_pipeline_heartbeat(*, force: bool = False) -> None:
        nonlocal last_pipeline_heartbeat_monotonic
        if run_log is None:
            return
        now = time.monotonic()
        if (
            not force
            and now - last_pipeline_heartbeat_monotonic
            < PIPELINE_HEARTBEAT_INTERVAL_SECONDS
        ):
            return
        last_pipeline_heartbeat_monotonic = now
        run_log.write_line(
                "heartbeat "
                f"fetched={fetched_count} "
                f"parsed={processed} "
                f"input_queue={queue_depth(input_queue)} "
                f"fetched_queue={queue_depth(fetched_queue)} "
                f"result_queue={queue_depth(result_queue)} "
                f"pdf_no_text_layer={pdf_no_text_layer_count} "
                f"errors={total_errors} "
                f"progress={_progress_text(processed, total_requested)}"
        )
        run_log.flush_and_fsync()

    def record_fetched_row(row: FetchedFinancialRow) -> None:
        nonlocal fetched_count
        if fetch_staging_writer is None:
            return
        with fetch_manifest_lock:
            fetch_staging_writer.append(row)
            fetch_staging_writer.flush_and_fsync()
        with counters_lock:
            fetched_count += 1

    def record_worker_failure(exc: BaseException) -> None:
        worker_failures.append(exc)
        request_shutdown(f"worker_failure:{type(exc).__name__}")

    def enqueue_input_item(item: FinancialTarget | None) -> bool:
        assert input_queue is not None
        while True:
            note_external_cancel()
            if shutdown_event.is_set():
                return False
            try:
                input_queue.put(item, timeout=0.25)
                return True
            except queue.Full:
                note_shutdown_if_needed()
                maybe_log_pipeline_heartbeat(force=False)
                continue

    def handle_signal(signum: int, _frame: object) -> None:
        try:
            signal_name = signal.Signals(signum).name
        except ValueError:
            signal_name = str(signum)
        request_shutdown(f"signal:{signal_name}")

    if threading.current_thread() is threading.main_thread():
        for signum in (signal.SIGINT, signal.SIGTERM):
            original_signal_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, handle_signal)

    parser_context = mp.get_context("fork")

    def fetch_worker() -> None:
        nonlocal fetch_workers_remaining
        assert input_queue is not None
        assert fetched_queue is not None
        try:
            with CompaniesHouseFinancialsClient(
                api_key=api_key,
                throttle=throttle,
            ) as client:
                while True:
                    note_external_cancel()
                    if shutdown_event.is_set():
                        break
                    try:
                        item = input_queue.get(timeout=0.25)
                    except queue.Empty:
                        continue
                    if item is None:
                        break
                    fetch_kwargs: dict[str, object] = {
                        "client": client,
                        "target": item,
                        "data_dir": data_dir,
                    }
                    if cancel_event is not None:
                        fetch_kwargs["cancel_event"] = cancel_event
                    try:
                        work_item = _fetch_company_work_item(**fetch_kwargs)
                    except OperationCancelled:
                        request_shutdown("cancelled")
                        break
                    record_fetched_row(work_item.row)
                    while True:
                        try:
                            fetched_queue.put(work_item, timeout=0.25)
                            break
                        except queue.Full:
                            note_external_cancel()
        except BaseException as exc:
            record_worker_failure(exc)
        finally:
            with fetch_workers_remaining_lock:
                fetch_workers_remaining -= 1
                is_last_fetch_worker = fetch_workers_remaining == 0
            if is_last_fetch_worker:
                assert fetched_queue is not None
                while True:
                    try:
                        fetched_queue.put(None, timeout=0.25)
                        break
                    except queue.Full:
                        if parser_controller_finished.is_set():
                            break

    def input_feeder() -> None:
        try:
            for target in targets:
                note_external_cancel()
                if not enqueue_input_item(target):
                    return
            for _ in range(worker_count):
                if not enqueue_input_item(None):
                    return
        except BaseException as exc:
            record_worker_failure(exc)

    def parser_controller() -> None:
        assert fetched_queue is not None
        assert result_queue is not None
        connection_to_handle: dict[Connection, ParserProcessHandle] = {}
        fetch_complete = False
        try:
            for handle in parser_processes:
                connection_to_handle[handle.connection] = handle

            while True:
                note_external_cancel()
                idle_handles = [
                    handle
                    for handle in parser_processes
                    if handle.current_work_item is None
                ]
                while idle_handles and not fetch_complete:
                    try:
                        work_item = fetched_queue.get(timeout=0.25)
                    except queue.Empty:
                        break
                    if work_item is None:
                        fetch_complete = True
                        break
                    handle = idle_handles.pop()
                    handle.connection.send(work_item.row)
                    handle.current_work_item = work_item

                busy_connections = [
                    handle.connection
                    for handle in parser_processes
                    if handle.current_work_item is not None
                ]
                if busy_connections:
                    ready_connections = wait_for_connections(
                        busy_connections,
                        timeout=0.25,
                    )
                    for connection in ready_connections:
                        handle = connection_to_handle[connection]
                        work_item = handle.current_work_item
                        if work_item is None:
                            continue
                        response = connection.recv()
                        handle.current_work_item = None
                        if response.error is not None or response.row is None:
                            raise RuntimeError(
                                response.error or "parser worker returned no row"
                            )
                        result = FinancialsResult(
                            row=response.row,
                            http_status=work_item.http_status,
                            latency_seconds=time.monotonic()
                            - work_item.started_monotonic,
                        )
                        while True:
                            try:
                                result_queue.put(result, timeout=0.25)
                                break
                            except queue.Full:
                                note_external_cancel()
                                continue

                if fetch_complete and not any(
                    handle.current_work_item is not None
                    for handle in parser_processes
                ):
                    break
        except BaseException as exc:
            record_worker_failure(exc)
        finally:
            for handle in parser_processes:
                try:
                    handle.connection.send(None)
                except (BrokenPipeError, EOFError, OSError):
                    pass
            for handle in parser_processes:
                try:
                    handle.process.join()
                finally:
                    handle.connection.close()
            parser_controller_finished.set()
            while True:
                try:
                    result_queue.put(None, timeout=0.25)
                    break
                except queue.Full:
                    note_external_cancel()
                    continue

    threads: list[threading.Thread] = []
    try:
        for index in range(parser_worker_count):
            parent_connection, child_connection = parser_context.Pipe()
            process = parser_context.Process(
                target=_parser_process_main,
                args=(child_connection,),
                name=f"financials-parse-{batch_id}-{index + 1}",
                daemon=False,
            )
            process.start()
            child_connection.close()
            parser_processes.append(
                ParserProcessHandle(
                    process=process,
                    connection=parent_connection,
                )
            )

        fetch_workers_remaining = worker_count
        for _ in range(worker_count):
            thread = threading.Thread(
                target=fetch_worker,
                name=f"financials-fetch-{batch_id}-{len(threads) + 1}",
            )
            threads.append(thread)
            thread.start()

        parser_thread = threading.Thread(
            target=parser_controller,
            name=f"financials-parse-{batch_id}-1",
        )
        threads.append(parser_thread)
        parser_thread.start()

        feeder_thread = threading.Thread(
            target=input_feeder,
            name=f"financials-feed-{batch_id}-1",
        )
        threads.append(feeder_thread)
        feeder_thread.start()

        pending_failure: BaseException | None = None
        parser_finished = False
        assert result_queue is not None
        while not parser_finished:
            note_external_cancel()
            if worker_failures and pending_failure is None:
                pending_failure = worker_failures[0]
            try:
                result = result_queue.get(timeout=0.25)
            except queue.Empty:
                note_shutdown_if_needed()
                maybe_log_pipeline_heartbeat(force=False)
                continue
            note_shutdown_if_needed()
            maybe_log_pipeline_heartbeat(force=False)
            if result is None:
                parser_finished = True
                continue
            staging_writer.append(result.row)
            processed += 1
            staged_since_sync += 1
            if result.row.parse_status == "pdf_no_text_layer":
                pdf_no_text_layer_count += 1
            if result.row.parse_status in ERROR_PARSE_STATUSES:
                total_errors += 1
            run_log.write_line(
                "company_number="
                f"{result.row.company_number} "
                f"http_status={result.http_status if result.http_status is not None else 'none'} "
                f"parse_status={result.row.parse_status} "
                f"filing_format={result.row.filing_format or 'none'} "
                f"latency={result.latency_seconds:.2f}s "
                f"progress={_progress_text(processed, total_requested)}"
            )
            if staged_since_sync >= validated_batch_size:
                checkpoint_write_phase()

        for thread in threads:
            thread.join()
        if pending_failure is not None:
            raise pending_failure
        if worker_failures:
            raise worker_failures[0]

        maybe_log_pipeline_heartbeat(force=True)
        if shutdown_reason["value"] == "cancelled":
            checkpoint_write_phase()
            load_summary = load_financials_staging(
                data_dir,
                db_path,
                batch_id=batch_id,
                final_status="cancelled",
            )
            _archive_financials_fetch_manifest(
                data_dir,
                batch_id=batch_id,
            )
            elapsed_seconds = time.monotonic() - started_monotonic
            run_log.write_line(
                "cancelled "
                f"requested={total_requested} "
                f"records_fetched={load_summary['records_fetched']} "
                f"records_updated={load_summary['records_updated']} "
                f"errors={load_summary['error_count']} "
                f"elapsed={_duration_text(elapsed_seconds)}"
            )
            run_log.flush_and_fsync()
            cancellation_finalized = True
            raise OperationCancelled("financials enrichment cancelled")
        if shutdown_event.is_set():
            raise KeyboardInterrupt(shutdown_reason["value"] or "shutdown requested")
        checkpoint_write_phase()
        load_summary = load_financials_staging(
            data_dir,
            db_path,
            batch_id=batch_id,
        )
        _archive_financials_fetch_manifest(
            data_dir,
            batch_id=batch_id,
        )
        elapsed_seconds = time.monotonic() - started_monotonic
        run_log.write_line(
            "complete "
            f"requested={total_requested} "
            f"records_fetched={load_summary['records_fetched']} "
            f"records_updated={load_summary['records_updated']} "
            f"ok={load_summary['ok_count']} "
            f"partial={load_summary['partial_count']} "
            f"ixbrl={load_summary['ixbrl_count']} "
            f"pdf={load_summary['pdf_count']} "
            f"pdf_no_text_layer={load_summary['pdf_no_text_layer_count']} "
            f"no_filing={load_summary['no_filing_count']} "
            f"stale={load_summary['stale_count']} "
            f"errors={load_summary['error_count']} "
            f"elapsed={_duration_text(elapsed_seconds)}"
        )
        run_log.flush_and_fsync()
        return {
            "batch_id": batch_id,
            "requested": total_requested,
            "records_fetched": load_summary["records_fetched"],
            "records_updated": load_summary["records_updated"],
            "ok_count": load_summary["ok_count"],
            "partial_count": load_summary["partial_count"],
            "ixbrl_count": load_summary["ixbrl_count"],
            "pdf_count": load_summary["pdf_count"],
            "pdf_no_text_layer_count": load_summary["pdf_no_text_layer_count"],
            "no_filing_count": load_summary["no_filing_count"],
            "stale_count": load_summary["stale_count"],
            "error_count": load_summary["error_count"],
            "loaded_paths": load_summary["loaded_paths"],
            "log_path": str(run_log.path),
            "mode": validated_mode,
        }
    except OperationCancelled:
        request_shutdown("cancelled")
        note_shutdown_if_needed()
        for thread in threads:
            thread.join()
        if (
            not cancellation_finalized
            and batch_id is not None
        ):
            if run_log is not None:
                checkpoint_write_phase()
                maybe_log_pipeline_heartbeat(force=True)
            if processed or fetched_count:
                load_summary = load_financials_staging(
                    data_dir,
                    db_path,
                    batch_id=batch_id,
                    final_status="cancelled",
                )
                _archive_financials_fetch_manifest(
                    data_dir,
                    batch_id=batch_id,
                )
                if run_log is not None:
                    elapsed_seconds = time.monotonic() - started_monotonic
                    run_log.write_line(
                        "cancelled "
                        f"requested={total_requested} "
                        f"records_fetched={load_summary['records_fetched']} "
                        f"records_updated={load_summary['records_updated']} "
                        f"errors={load_summary['error_count']} "
                        f"elapsed={_duration_text(elapsed_seconds)}"
                    )
                    run_log.flush_and_fsync()
            else:
                def mark_cancelled(con: duckdb.DuckDBPyConnection) -> None:
                    ensure_pipeline_schema(con)
                    finish_sync_batch(
                        con,
                        batch_id,
                        status="cancelled",
                        records_fetched=0,
                        records_updated=0,
                        error_count=0,
                    )

                with_duckdb_connection(db_path, mark_cancelled)
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
        request_shutdown(shutdown_reason["value"] or "exception")
        note_shutdown_if_needed()
        for thread in threads:
            thread.join()
        if run_log is not None:
            checkpoint_write_phase()
            maybe_log_pipeline_heartbeat(force=True)
        if batch_id is not None:
            def mark_failed(con: duckdb.DuckDBPyConnection) -> None:
                ensure_pipeline_schema(con)
                finish_sync_batch(
                    con,
                    batch_id,
                    status="failed",
                    records_fetched=processed,
                    records_updated=0,
                    error_count=total_errors + 1,
                )

            with_duckdb_connection(db_path, mark_failed)
        if run_log is not None:
            run_log.write_line(
                "crash "
                f"processed={processed} "
                f"fetched={fetched_count} "
                f"errors={total_errors + 1} "
                f"reason={shutdown_reason['value'] or 'exception'}"
            )
            run_log.flush_and_fsync()
        raise
    finally:
        for handle in parser_processes:
            if not handle.connection.closed:
                try:
                    handle.connection.send(None)
                except (BrokenPipeError, EOFError, OSError):
                    pass
                handle.connection.close()
            if handle.process.is_alive():
                handle.process.join(timeout=1.0)
                if handle.process.is_alive():
                    handle.process.terminate()
                    handle.process.join(timeout=1.0)
        for signum, handler in original_signal_handlers.items():
            signal.signal(signum, handler)
        if run_log is not None:
            run_log.close()
        if staging_writer is not None:
            staging_writer.close()
        if fetch_staging_writer is not None:
            fetch_staging_writer.close()
