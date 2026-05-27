"""Test-side path helpers for isolated fixture and workspace setup."""

from pathlib import Path

FIXTURES_ROOT = Path(__file__).resolve().parent.parent / "fixtures"


def fixture_path(*parts: str) -> Path:
    """Resolve a path under tests/fixtures/."""
    return FIXTURES_ROOT.joinpath(*parts)


def make_test_workspace(
    tmpdir: str | Path,
    db_name: str = "test.duckdb",
) -> tuple[Path, Path]:
    """Build a clean (data_dir, db_path) pair under tmpdir."""
    data_dir = Path(tmpdir) / "data"
    db_dir = data_dir / "db"
    db_dir.mkdir(parents=True, exist_ok=True)
    return data_dir, db_dir / db_name
