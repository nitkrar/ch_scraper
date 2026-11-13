"""Centralized path constants for ch_bulk."""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = REPO_ROOT / "ch_bulk" / "db" / "sql"


def _default_data_dir(
    *,
    package_root: Path = REPO_ROOT,
    home: Path | None = None,
    frozen: bool | None = None,
) -> Path:
    """Return the default data directory.

    In a source checkout or a frozen (PyInstaller) app, data lives
    next to the package, preserving existing behaviour. For a
    regular installed package there is no checkout, and writing
    under the install prefix is wrong, so data goes under the
    user's home directory.
    """
    if frozen is None:
        frozen = bool(getattr(sys, "frozen", False))
    if frozen or (package_root / "pyproject.toml").exists():
        return package_root / "data"
    home_dir = home if home is not None else Path.home()
    return home_dir / ".ch_bulk" / "data"


DEFAULT_DATA_DIR = _default_data_dir()
DATA_REFERENCE_DIR = DEFAULT_DATA_DIR / "reference"


def input_dir(data_dir: Path | str, source: str) -> Path:
    return Path(data_dir) / "input" / source


def ch_input_dir(data_dir: Path | str) -> Path:
    return input_dir(data_dir, "ch")


def cqc_input_dir(data_dir: Path | str) -> Path:
    return input_dir(data_dir, "cqc")


def settings_path(data_dir: Path | str) -> Path:
    return Path(data_dir) / "settings.json"


def reference_dir(data_dir: Path | str) -> Path:
    return Path(data_dir) / "reference"


def revenue_bands_path(data_dir: Path | str) -> Path:
    return reference_dir(data_dir) / "revenue_bands.csv"


def db_dir(data_dir: Path | str) -> Path:
    return Path(data_dir) / "db"


def default_db_path(data_dir: Path | str) -> Path:
    return db_dir(data_dir) / "ch_bulk.duckdb"


DEFAULT_DB_PATH = default_db_path(DEFAULT_DATA_DIR)


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
