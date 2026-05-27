"""Centralized path constants for ch_bulk."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = REPO_ROOT / "ch_bulk" / "db" / "sql"
DEFAULT_DATA_DIR = REPO_ROOT / "data"
DATA_REFERENCE_DIR = DEFAULT_DATA_DIR / "reference"
DEFAULT_DB_PATH = DEFAULT_DATA_DIR / "db" / "ch_bulk.duckdb"


def logs_dir(data_dir: Path | str) -> Path:
    return Path(data_dir) / "logs"


def staging_root(data_dir: Path | str) -> Path:
    return Path(data_dir) / "staging"


def runs_dir(data_dir: Path | str, sync_type: str) -> Path:
    return staging_root(data_dir) / "runs" / sync_type


def run_stage_file(data_dir: Path | str, sync_type: str, batch_id: str) -> Path:
    return runs_dir(data_dir, sync_type) / f"{batch_id}.jsonl"


def raw_dir(data_dir: Path | str, source: str) -> Path:
    return staging_root(data_dir) / "raw" / source


def raw_filings_dir(data_dir: Path | str, company_number: str) -> Path:
    return raw_dir(data_dir, "companies_house") / "filings" / company_number


def derived_dir(data_dir: Path | str, domain: str) -> Path:
    return staging_root(data_dir) / "derived" / domain


def archive_dir(data_dir: Path | str, source: str) -> Path:
    return Path(data_dir) / "archive" / source


def validation_dir(data_dir: Path | str) -> Path:
    return staging_root(data_dir) / "validation"
