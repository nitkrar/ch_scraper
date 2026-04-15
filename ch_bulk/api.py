"""High-level Python API for Companies House bulk data."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

import duckdb

from ch_bulk.downloader import download_bulk_data
from ch_bulk.processor import process_csvs
from ch_bulk.query import export_query_csv, get_db_info, query_by_sic
from ch_bulk.query import export_filtered_csv as _export_filtered_csv
from ch_bulk.query import get_filter_options as _get_filter_options
from ch_bulk.query import query_companies

logger = logging.getLogger(__name__)


class ChBulk:
    """Primary interface for downloading, processing, and querying
    UK Companies House bulk CSV data.

    Example::

        ch = ChBulk(data_dir="./data", db_path="ch_bulk.duckdb")
        ch.download()
        ch.process()
        companies = ch.query("62012")

    Args:
        data_dir: Directory to store downloaded/extracted files.
        db_path: Path to the DuckDB database file.
    """

    def __init__(
        self,
        data_dir: str | Path = "./data",
        db_path: str | Path = "ch_bulk.duckdb",
    ) -> None:
        self.data_dir = Path(data_dir)
        self.db_path = Path(db_path)

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
    ) -> int:
        """Ingest downloaded CSV files into DuckDB.

        Args:
            csv_files: Specific CSV files to ingest. If ``None``,
                auto-discovers all ``BasicCompanyData*.csv`` files
                in :attr:`data_dir`.
            progress_callback: If provided, called with status strings
                instead of printing to the terminal.

        Returns:
            Total number of rows ingested.

        Raises:
            FileNotFoundError: If no CSV files are found.
        """
        if csv_files is None:
            csv_files = sorted(self.data_dir.glob("BasicCompanyData*.csv"))
        if not csv_files:
            raise FileNotFoundError(
                f"No BasicCompanyData CSV files found in {self.data_dir}"
            )
        return process_csvs(csv_files, self.db_path, progress_callback=progress_callback)

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
    ) -> int:
        """Download and process in one step.

        Passes the exact CSV files from download to process, avoiding
        stale files from previous months being mixed in.

        Args:
            month: Month in ``YYYY-MM`` format, or ``None`` to auto-detect.
            keep_zips: If ``True``, keep ZIP files after extraction.

        Returns:
            Total number of rows ingested.
        """
        csv_files = self.download(month=month, keep_zips=keep_zips)
        return self.process(csv_files=csv_files)

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

        con = duckdb.connect(str(self.db_path), read_only=True)
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

    def export_filtered_csv(self, output_path, **filters) -> int:
        """Export filtered results to CSV via DuckDB COPY. Returns row count."""
        return _export_filtered_csv(self.db_path, output_path, **filters)
