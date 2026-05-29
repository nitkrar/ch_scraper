"""High-level Python API for Companies House bulk data."""

from __future__ import annotations

import logging
import os
import re
import threading
from datetime import datetime
from pathlib import Path

import duckdb

from ch_bulk.core.cancellation import OperationCancelled
from ch_bulk.core.logging import setup_logging
from ch_bulk.db.bootstrap import ensure_pipeline_schema, recover_interrupted_compaction
from ch_bulk.companies_house.ch_enricher import enrich_directors as _enrich_directors
from ch_bulk.companies_house.ch_enricher import load_director_staging as _load_director_staging
from ch_bulk.web.classifier import WebsiteClassifier, load_classification_staging as _load_classification_staging
from ch_bulk.companies_house.ch_enricher import enrich_revenue as _enrich_revenue
from ch_bulk.core.paths import (
    DEFAULT_DATA_DIR,
    DEFAULT_DB_PATH,
    ch_input_dir,
    cqc_input_dir,
    default_db_path,
)
from ch_bulk.companies_house.financials_enricher import enrich_financials as _enrich_financials
from ch_bulk.companies_house.financials_enricher import load_financials_staging as _load_financials_staging
from ch_bulk.cqc.downloader import download_cqc_directory, download_hsca_filters
from ch_bulk.cqc.api_enricher import CQCAPIEnricher, load_cqc_staging as _load_cqc_staging
from ch_bulk.cqc.processor import process_cqc_csv, process_hsca_filters
from ch_bulk.cqc.query import (
    export_cqc_locations_csv as _export_cqc_locations_csv,
    export_cqc_providers_csv as _export_cqc_providers_csv,
    export_hsca_locations_csv as _export_hsca_locations_csv,
    get_cqc_filter_options as _get_cqc_filter_options,
    query_cqc_locations,
    query_cqc_providers,
    query_hsca_locations,
)
from ch_bulk.companies_house.downloader import download_bulk_data
from ch_bulk.matching.ch_cqc import match_companies_to_cqc
from ch_bulk.db.migration import export_to_parquet as _export_to_parquet
from ch_bulk.db.migration import import_from_parquet as _import_from_parquet
from ch_bulk.companies_house.processor import (
    SanityCheckError,
    SanityCheckResult,
    compact_database,
    process_csvs,
)
from ch_bulk.companies_house.query import export_query_csv, get_db_info, query_by_sic
from ch_bulk.companies_house.query import export_directors_age_csv as _export_directors_age_csv
from ch_bulk.companies_house.query import export_filtered_csv as _export_filtered_csv
from ch_bulk.companies_house.query import export_financials_csv as _export_financials_csv
from ch_bulk.companies_house.query import get_filter_options as _get_filter_options
from ch_bulk.companies_house.query import query_directors_age
from ch_bulk.companies_house.query import query_financials
from ch_bulk.companies_house.query import query_companies
from ch_bulk.web.website_finder import (
    WebsiteFinder,
    load_website_finder_staging as _load_website_finder_staging,
)

logger = logging.getLogger(__name__)

# CH bulk filenames look like:
#   BasicCompanyData-2026-05-01-part3_7.csv
# Group by the YYYY-MM-DD chunk so multi-month folders process in order.
_MONTH_RE = re.compile(r"BasicCompanyData-(\d{4}-\d{2}-\d{2})-part\d+_\d+\.csv$")


def _group_by_month(csv_files: list[Path]) -> dict[str, list[Path]]:
    """Group CH bulk CSV files by their YYYY-MM-DD prefix.

    Files whose names don't match the expected pattern are bucketed
    under ``"unknown"`` — they'll still be processed, but as a single
    chunk after all dated months.
    """
    groups: dict[str, list[Path]] = {}
    for f in csv_files:
        m = _MONTH_RE.match(f.name)
        key = m.group(1) if m else "unknown"
        groups.setdefault(key, []).append(f)
    for v in groups.values():
        v.sort()
    return groups


class ChBulk:
    """Primary interface for downloading, processing, and querying
    UK Companies House bulk CSV data.

    Example::

        from ch_bulk.core.paths import DEFAULT_DATA_DIR, DEFAULT_DB_PATH

        ch = ChBulk(data_dir=DEFAULT_DATA_DIR, db_path=DEFAULT_DB_PATH)
        ch.download()
        ch.process()
        companies = ch.query("62012")

    Args:
        data_dir: Directory to store downloaded/extracted files. CH bulk
            files land in ``<data_dir>/input/ch/``; CQC files (when added)
            will land in ``<data_dir>/input/cqc/``.
        db_path: Path to the DuckDB database file.
    """

    def __init__(
        self,
        data_dir: str | Path | None = None,
        db_path: str | Path | None = None,
    ) -> None:
        self.data_dir = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
        self.db_path = Path(db_path) if db_path is not None else default_db_path(self.data_dir)
        setup_logging(self.data_dir)

    @property
    def ch_dir(self) -> Path:
        """Where CH BasicCompanyData CSVs live."""
        return ch_input_dir(self.data_dir)

    @property
    def cqc_dir(self) -> Path:
        """Where CQC bulk files will live (when CQC support lands)."""
        return cqc_input_dir(self.data_dir)

    def download(
        self,
        month: str | None = None,
        keep_zips: bool = False,
        progress_callback: callable | None = None,
    ) -> list[Path]:
        """Download Companies House bulk CSV data.

        Args:
            month: Month in ``YYYY-MM`` format, or ``None`` to auto-detect.
            keep_zips: If ``True``, keep ZIP files after extraction.
            progress_callback: If provided, called with status strings
                instead of showing rich Progress bars in the terminal.

        Returns:
            List of paths to extracted CSV files.
        """
        return download_bulk_data(
            data_dir=self.data_dir,
            month=month,
            keep_zips=keep_zips,
            progress_callback=progress_callback,
        )

    def process(
        self,
        csv_files: list[Path] | None = None,
        progress_callback: callable | None = None,
        compact: bool = True,
        force: bool = False,
        cancel_event: threading.Event | None = None,
    ) -> int:
        """Ingest downloaded CSV files into DuckDB via upsert.

        First call (no ``companies`` table): bootstraps the table from
        the oldest month present, then upserts each newer month in
        order. Subsequent calls: merges via the upsert pipeline, with
        two sanity checks gating the merge — row-count delta and
        inactive-churn delta, both at 5% thresholds.

        Args:
            csv_files: Specific CSV files to ingest. If ``None``,
                auto-discovers all ``BasicCompanyData*.csv`` files in
                ``<data_dir>/input/ch/``. When multiple months are
                present, processes the oldest first then upserts the
                rest in chronological order.
            progress_callback: If provided, called with status strings
                instead of printing to the terminal.
            compact: If ``True`` (default), reclaim disk space after
                ingest by rebuilding the database file. DuckDB does
                not reclaim pages after table changes automatically.
            force: If ``True``, skip the sanity-check guard. Use only
                when you've verified the input is correct (e.g., genuine
                large attrition month, or a deliberate schema change).

        Returns:
            Total number of rows in ``companies`` after all months are
            processed.

        Raises:
            FileNotFoundError: If no CSV files are found.
            ch_bulk.companies_house.processor.SanityCheckError: If a sanity check fails
                and ``force=False``. Inspect ``.result`` for the numbers.
        """
        if csv_files is None:
            csv_files = sorted(self.ch_dir.glob("BasicCompanyData*.csv"))
        else:
            csv_files = [Path(f) for f in csv_files]
        if not csv_files:
            raise FileNotFoundError(
                f"No BasicCompanyData CSV files found in {self.ch_dir}"
            )

        # Group by month so multiple months get processed in order
        # (oldest bootstraps, newer ones upsert). For a single-month
        # call this is just one group.
        months = _group_by_month(csv_files)
        row_count = 0
        for month_key in sorted(months.keys()):
            if cancel_event is not None and cancel_event.is_set():
                raise OperationCancelled("CH bulk processing cancelled")
            month_files = months[month_key]
            if progress_callback:
                progress_callback(
                    f"Processing month {month_key} "
                    f"({len(month_files)} files)..."
                )
            # Defer compaction until after all months — no point
            # compacting between back-to-back upserts.
            row_count = process_csvs(
                month_files,
                self.db_path,
                progress_callback=progress_callback,
                force=force,
                compact=False,
                cancel_event=cancel_event,
            )
        if compact:
            if cancel_event is not None and cancel_event.is_set():
                raise OperationCancelled("CH bulk processing cancelled")
            compact_database(
                self.db_path,
                progress_callback=progress_callback,
                cancel_event=cancel_event,
            )
        return row_count

    def compact(
        self,
        progress_callback: callable | None = None,
        cancel_event: threading.Event | None = None,
    ) -> None:
        """Rebuild the database file to reclaim disk space.

        Use this after manually editing the database (e.g., dropping
        a table via raw DuckDB) when you want to shrink the file on
        disk. ``process()`` calls this automatically by default.

        Args:
            progress_callback: If provided, called with status strings.

        Raises:
            FileNotFoundError: If the database does not exist.
        """
        if cancel_event is not None and cancel_event.is_set():
            raise OperationCancelled("database compaction cancelled")
        compact_database(
            self.db_path,
            progress_callback=progress_callback,
            cancel_event=cancel_event,
        )

    def bootstrap(self) -> None:
        """Create the extended homecare pipeline schema objects.

        Safe to re-run. If the CH ``companies`` table does not exist yet,
        the table bootstrap still succeeds and the derived views are
        deferred until a later call.
        """
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        recover_interrupted_compaction(self.db_path)
        con = duckdb.connect(str(self.db_path))
        try:
            ensure_pipeline_schema(con)
        finally:
            con.close()

    # ── CQC pipeline ─────────────────────────────────────────────────

    def download_cqc(
        self,
        progress_callback: callable | None = None,
    ) -> Path:
        """Download the latest CQC care directory CSV.

        Saved to ``<data_dir>/input/cqc/cqc_directory_YYYY-MM-DD.csv``.
        Idempotent: if a file for the latest published date is already
        on disk, the download is skipped.

        Returns:
            Path to the downloaded (or already-present) CSV.
        """
        return download_cqc_directory(
            self.data_dir, progress_callback=progress_callback
        )

    def process_cqc(
        self,
        csv_file: Path | None = None,
        progress_callback: callable | None = None,
        compact: bool = True,
        force: bool = False,
        cancel_event: threading.Event | None = None,
    ) -> int:
        """Ingest CQC directory CSV into the DuckDB database.

        Auto-discovers the latest CSV in ``<data_dir>/input/cqc/`` if
        ``csv_file`` is not supplied.

        Returns:
            Total rows in ``cqc_locations`` after the operation.

        Raises:
            FileNotFoundError: If no CQC CSV is found.
            ch_bulk.companies_house.processor.SanityCheckError: On sanity failure.
        """
        if csv_file is None:
            candidates = sorted(self.cqc_dir.glob("cqc_directory_*.csv"))
            if not candidates:
                raise FileNotFoundError(
                    f"No cqc_directory_*.csv files found in {self.cqc_dir}"
                )
            csv_file = candidates[-1]
        return process_cqc_csv(
            csv_file,
            self.db_path,
            progress_callback=progress_callback,
            force=force,
            compact=compact,
            cancel_event=cancel_event,
        )

    def sync_cqc(
        self,
        force: bool = False,
        progress_callback: callable | None = None,
        cancel_event: threading.Event | None = None,
    ) -> int:
        """Download + process the latest CQC directory in one step."""
        csv_file = self.download_cqc(progress_callback=progress_callback)
        return self.process_cqc(
            csv_file=csv_file,
            progress_callback=progress_callback,
            force=force,
            cancel_event=cancel_event,
        )

    def download_hsca(
        self,
        target_date=None,
        progress_callback: callable | None = None,
    ) -> Path:
        """Download the latest HSCA active locations ODS."""
        return download_hsca_filters(
            self.data_dir,
            target_date=target_date,
            progress_callback=progress_callback,
        )

    def process_hsca(
        self,
        ods_file: Path | None = None,
        progress_callback: callable | None = None,
        compact: bool = True,
        force: bool = False,
        cancel_event: threading.Event | None = None,
    ) -> int:
        """Ingest the latest HSCA ODS into the DuckDB database."""
        if ods_file is None:
            candidates = sorted(self.cqc_dir.glob("hsca_active_locations_*.ods"))
            if not candidates:
                raise FileNotFoundError(
                    f"No hsca_active_locations_*.ods files found in {self.cqc_dir}"
                )
            ods_file = candidates[-1]
        return process_hsca_filters(
            ods_file,
            self.db_path,
            progress_callback=progress_callback,
            force=force,
            compact=compact,
            cancel_event=cancel_event,
        )

    def cqc_hsca_sync(
        self,
        force: bool = False,
        target_date=None,
        progress_callback: callable | None = None,
        cancel_event: threading.Event | None = None,
    ) -> int:
        """Download + process the latest HSCA ODS in one step."""
        ods_file = self.download_hsca(
            target_date=target_date,
            progress_callback=progress_callback,
        )
        return self.process_hsca(
            ods_file=ods_file,
            progress_callback=progress_callback,
            force=force,
            cancel_event=cancel_event,
        )

    def cqc_enrich_providers(
        self,
        *,
        mode: str = "incremental",
        ids: list[str] | None = None,
        batch_size: int = 1000,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, object]:
        enricher = CQCAPIEnricher(self.data_dir, self.db_path)
        if cancel_event is None:
            return enricher.enrich_providers(mode=mode, ids=ids, batch_size=batch_size)
        return enricher.enrich_providers(
            mode=mode,
            ids=ids,
            batch_size=batch_size,
            cancel_event=cancel_event,
        )

    def cqc_enrich_locations(
        self,
        *,
        mode: str = "incremental",
        ids: list[str] | None = None,
        batch_size: int = 1000,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, object]:
        enricher = CQCAPIEnricher(self.data_dir, self.db_path)
        if cancel_event is None:
            return enricher.enrich_locations(mode=mode, ids=ids, batch_size=batch_size)
        return enricher.enrich_locations(
            mode=mode,
            ids=ids,
            batch_size=batch_size,
            cancel_event=cancel_event,
        )

    def ch_enrich_directors(
        self,
        *,
        sic: str = "88100",
        company_numbers: list[str] | None = None,
        force: bool = False,
        batch_size: int = 1000,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, int | str]:
        request_kwargs: dict[str, object] = {
            "sic": sic,
            "company_numbers": company_numbers,
            "force": force,
            "batch_size": batch_size,
        }
        if cancel_event is not None:
            request_kwargs["cancel_event"] = cancel_event
        return _enrich_directors(
            self.db_path,
            self.data_dir,
            **request_kwargs,
        )

    def ch_enrich_revenue(
        self,
        *,
        sic: str = "88100",
        company_numbers: list[str] | None = None,
    ) -> dict[str, int]:
        return _enrich_revenue(
            self.db_path,
            self.data_dir,
            sic=sic,
            company_numbers=company_numbers,
        )

    def enrich_financials(
        self,
        *,
        mode: str = "incremental",
        ids: list[str] | None = None,
        workers: int = 3,
        parser_workers: int = 4,
        batch_size: int = 100,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, object]:
        request_kwargs: dict[str, object] = {
            "mode": mode,
            "ids": ids,
            "workers": workers,
            "parser_workers": parser_workers,
            "batch_size": batch_size,
        }
        if cancel_event is not None:
            request_kwargs["cancel_event"] = cancel_event
        return _enrich_financials(
            self.db_path,
            self.data_dir,
            **request_kwargs,
        )

    def classify(
        self,
        *,
        mode: str = "incremental",
        ids: list[str] | None = None,
        batch_size: int = 100,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, object]:
        with WebsiteClassifier(self.data_dir, self.db_path) as classifier:
            request_kwargs: dict[str, object] = {
                "mode": mode,
                "ids": ids,
                "batch_size": batch_size,
            }
            if cancel_event is not None:
                request_kwargs["cancel_event"] = cancel_event
            return classifier.classify(**request_kwargs)

    def find_websites(
        self,
        *,
        mode: str = "incremental",
        ids: list[str] | None = None,
        pause_seconds: float = 0.7,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, object]:
        with WebsiteFinder(self.data_dir, self.db_path) as finder:
            request_kwargs: dict[str, object] = {
                "mode": mode,
                "ids": ids,
                "pause_seconds": pause_seconds,
            }
            if cancel_event is not None:
                request_kwargs["cancel_event"] = cancel_event
            return finder.find(**request_kwargs)

    def migration_export(
        self,
        *,
        bundle_dir: str | Path,
    ) -> dict[str, object]:
        return _export_to_parquet(self.db_path, bundle_dir)

    def migration_import(
        self,
        *,
        bundle_dir: str | Path,
        force: bool = False,
    ) -> dict[str, object]:
        return _import_from_parquet(
            bundle_dir,
            self.db_path,
            force=force,
        )

    def load_staging(
        self,
        *,
        sync_type: str,
        batch_id: str | None = None,
    ) -> dict[str, object]:
        if sync_type in {"api_providers", "api_locations"}:
            return _load_cqc_staging(
                self.data_dir,
                self.db_path,
                sync_type=sync_type,
                batch_id=batch_id,
            )
        if sync_type == "classifications":
            return _load_classification_staging(
                self.data_dir,
                self.db_path,
                batch_id=batch_id,
            )
        if sync_type == "website_finder":
            return _load_website_finder_staging(
                self.data_dir,
                self.db_path,
                batch_id=batch_id,
            )
        if sync_type == "ch_directors":
            return _load_director_staging(
                self.data_dir,
                self.db_path,
                batch_id=batch_id,
            )
        if sync_type == "financials":
            return _load_financials_staging(
                self.data_dir,
                self.db_path,
                batch_id=batch_id,
            )
        raise ValueError(f"Unsupported sync_type for load-staging: {sync_type}")

    def match(
        self,
        *,
        mode: str = "incremental",
    ) -> dict[str, int | str]:
        return match_companies_to_cqc(
            self.db_path,
            mode=mode,
        )

    def query(
        self,
        sic_codes: str | list[str],
        status: str | None = "Active",
        limit: int | None = None,
        output_csv: str | Path | None = None,
    ) -> list[dict]:
        """Query companies by SIC code.

        Args:
            sic_codes: One or more SIC codes (string, comma-separated
                string, or list).
            status: Filter by company status, or ``None`` for all.
            limit: Maximum results to return, or ``None`` for all.
            output_csv: If provided, also export results to this CSV path.

        Returns:
            List of company records as dictionaries.
        """
        if output_csv:
            # Stream directly to CSV via DuckDB COPY (no OOM risk)
            export_query_csv(
                self.db_path, sic_codes, output_csv,
                status=status, limit=limit,
            )
            # Don't also load into memory — return empty list
            # Caller can query separately with a limit if they
            # need in-memory results too
            return []
        return query_by_sic(
            self.db_path, sic_codes, status=status, limit=limit
        )

    def sync(
        self,
        month: str | None = None,
        keep_zips: bool = False,
        force: bool = False,
        cancel_event: threading.Event | None = None,
    ) -> int:
        """Download and process in one step.

        Passes the exact CSV files from download to process, avoiding
        stale files from previous months being mixed in.

        Args:
            month: Month in ``YYYY-MM`` format, or ``None`` to auto-detect.
            keep_zips: If ``True``, keep ZIP files after extraction.
            force: If ``True``, skip the upsert sanity-check guard.

        Returns:
            Total number of rows ingested.
        """
        csv_files = self.download(month=month, keep_zips=keep_zips)
        return self.process(
            csv_files=csv_files,
            force=force,
            cancel_event=cancel_event,
        )

    def export_sqlite(self, output_path: str | Path) -> Path:
        """Export the DuckDB database to a SQLite file.

        Uses DuckDB's built-in SQLite extension to copy the
        ``companies`` table.

        Args:
            output_path: Path for the output SQLite file.

        Returns:
            Path to the created SQLite file.

        Raises:
            FileNotFoundError: If the DuckDB database does not exist.
        """
        output_path = Path(output_path)
        if not self.db_path.exists():
            raise FileNotFoundError(
                f"DuckDB database not found: {self.db_path}"
            )

        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Escape path for safe SQL embedding (backslashes then single quotes)
        safe_path = str(output_path).replace("\\", "\\\\").replace("'", "''")

        recover_interrupted_compaction(self.db_path)
        con = duckdb.connect(str(self.db_path))
        try:
            con.execute("INSTALL sqlite; LOAD sqlite;")
            con.execute(
                f"ATTACH '{safe_path}' AS sqlite_db (TYPE SQLITE)"
            )
            con.execute(
                "CREATE TABLE sqlite_db.companies AS "
                "SELECT * FROM companies"
            )
            con.execute("DETACH sqlite_db")
            logger.info("Exported database to %s", output_path)
        except duckdb.IOException as exc:
            raise RuntimeError(
                f"Database error during SQLite export: {exc}"
            ) from exc
        finally:
            con.close()

        return output_path

    def info(self) -> dict:
        """Get summary statistics about the database.

        Returns:
            Dictionary with ``total_companies``, ``status_breakdown``,
            ``top_sic_codes``, and ``db_file_modified``.

        Raises:
            FileNotFoundError: If the database does not exist.
        """
        stats = get_db_info(self.db_path)
        stats["db_file_modified"] = (
            datetime.fromtimestamp(os.path.getmtime(self.db_path)).isoformat()
            if self.db_path.exists() else None
        )
        return stats

    def query_advanced(self, **filters) -> tuple[list[dict], int]:
        """Multi-filter paginated query. Returns (rows, total_count)."""
        return query_companies(self.db_path, **filters)

    def get_filter_options(self) -> dict:
        """Returns distinct values for filter dropdowns."""
        return _get_filter_options(self.db_path)

    def query_cqc_locations_advanced(self, **filters) -> tuple[list[dict], int]:
        """Multi-filter paginated query against cqc_locations."""
        return query_cqc_locations(self.db_path, **filters)

    def query_cqc_providers_advanced(self, **filters) -> tuple[list[dict], int]:
        """Multi-filter paginated query against cqc_providers."""
        return query_cqc_providers(self.db_path, **filters)

    def query_hsca_locations_advanced(self, **filters) -> tuple[list[dict], int]:
        """Multi-filter paginated query against joined HSCA locations."""
        return query_hsca_locations(self.db_path, **filters)

    def query_directors_age_advanced(self, **filters) -> tuple[list[dict], int]:
        """Multi-filter paginated query against CH director-age enrichment rows."""
        return query_directors_age(self.db_path, **filters)

    def query_financials_advanced(self, **filters) -> tuple[list[dict], int]:
        """Multi-filter paginated query against CH financial enrichment rows."""
        return query_financials(self.db_path, **filters)

    def get_cqc_filter_options(self) -> dict:
        """Distinct values for CQC filter dropdowns."""
        return _get_cqc_filter_options(self.db_path)

    def export_cqc_locations_csv(self, output_path, **filters) -> int:
        """Export filtered cqc_locations to CSV. Returns row count."""
        return _export_cqc_locations_csv(self.db_path, output_path, **filters)

    def export_cqc_providers_csv(self, output_path, **filters) -> int:
        """Export filtered cqc_providers to CSV. Returns row count."""
        return _export_cqc_providers_csv(self.db_path, output_path, **filters)

    def export_hsca_locations_csv(self, output_path, **filters) -> int:
        """Export filtered joined HSCA locations to CSV. Returns row count."""
        return _export_hsca_locations_csv(self.db_path, output_path, **filters)

    def export_directors_age_csv(self, output_path, **filters) -> int:
        """Export filtered CH director-age enrichment rows to CSV."""
        return _export_directors_age_csv(self.db_path, output_path, **filters)

    def export_financials_csv(self, output_path, **filters) -> int:
        """Export filtered CH financial enrichment rows to CSV."""
        return _export_financials_csv(self.db_path, output_path, **filters)

    def export_filtered_csv(self, output_path, **filters) -> int:
        """Export filtered results to CSV via DuckDB COPY. Returns row count."""
        return _export_filtered_csv(self.db_path, output_path, **filters)
