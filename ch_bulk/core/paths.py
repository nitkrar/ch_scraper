"""Centralized path constants for ch_bulk."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = REPO_ROOT / "sql"
DEFAULT_DATA_DIR = REPO_ROOT / "data"
DATA_REFERENCE_DIR = DEFAULT_DATA_DIR / "reference"
DEFAULT_DB_PATH = DEFAULT_DATA_DIR / "db" / "ch_bulk.duckdb"
