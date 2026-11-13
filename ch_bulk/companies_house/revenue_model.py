"""Deterministic employee-count to revenue estimate model."""

from __future__ import annotations

import csv
import io
import sys
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

from ch_bulk.core.paths import revenue_bands_path

BANDS_CSV_FIELDS: tuple[str, ...] = (
    "min_employees",
    "max_employees",
    "low_gbp",
    "high_gbp",
    "midpoint_gbp",
)


@dataclass(frozen=True)
class RevenueBand:
    min_employees: int
    max_employees: int
    low_gbp: float
    high_gbp: float
    midpoint_gbp: float


def _format_number(value: float) -> str:
    if value == int(value):
        return str(int(value))
    return repr(value)


def _band_from_row(row: dict[str, str | None], *, line_no: int) -> RevenueBand:
    try:
        min_employees = int((row["min_employees"] or "").strip())
        max_raw = (row["max_employees"] or "").strip()
        max_employees = int(max_raw) if max_raw else sys.maxsize
        low_gbp = float((row["low_gbp"] or "").strip())
        high_gbp = float((row["high_gbp"] or "").strip())
        midpoint_gbp = float((row["midpoint_gbp"] or "").strip())
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"invalid revenue band on line {line_no}: {exc}") from exc
    if min_employees < 0:
        raise ValueError(f"invalid revenue band on line {line_no}: negative min_employees")
    if max_employees < min_employees:
        raise ValueError(
            f"invalid revenue band on line {line_no}: max_employees < min_employees"
        )
    if low_gbp < 0 or high_gbp < 0 or midpoint_gbp < 0:
        raise ValueError(f"invalid revenue band on line {line_no}: negative amount")
    if high_gbp < low_gbp:
        raise ValueError(f"invalid revenue band on line {line_no}: high_gbp < low_gbp")
    return RevenueBand(min_employees, max_employees, low_gbp, high_gbp, midpoint_gbp)


def parse_revenue_bands_text(text: str) -> list[RevenueBand]:
    """Parse revenue-band CSV text, raising ValueError on bad input."""
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise ValueError("revenue bands CSV is empty")
    missing = [field for field in BANDS_CSV_FIELDS if field not in reader.fieldnames]
    if missing:
        raise ValueError(f"revenue bands CSV is missing columns: {', '.join(missing)}")
    bands: list[RevenueBand] = []
    for line_no, row in enumerate(reader, start=2):
        if all(not (row.get(field) or "").strip() for field in BANDS_CSV_FIELDS):
            continue
        bands.append(_band_from_row(row, line_no=line_no))
    if not bands:
        raise ValueError("revenue bands CSV has no bands")
    return bands


def load_bands(csv_path: Path) -> list[RevenueBand]:
    return parse_revenue_bands_text(csv_path.read_text(encoding="utf-8"))


def bands_to_csv_text(bands: list[RevenueBand]) -> str:
    """Render bands as CSV text. An open-ended max is written blank."""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=BANDS_CSV_FIELDS, lineterminator="\n")
    writer.writeheader()
    for band in bands:
        writer.writerow(
            {
                "min_employees": band.min_employees,
                "max_employees": (
                    "" if band.max_employees >= sys.maxsize else band.max_employees
                ),
                "low_gbp": _format_number(band.low_gbp),
                "high_gbp": _format_number(band.high_gbp),
                "midpoint_gbp": _format_number(band.midpoint_gbp),
            }
        )
    return output.getvalue()


def _packaged_bands_text() -> str:
    traversable = resources.files("ch_bulk").joinpath("data").joinpath("revenue_bands.csv")
    try:
        if traversable.is_file():
            return traversable.read_text(encoding="utf-8")
    except (OSError, RuntimeError):
        pass
    fallback = Path(__file__).resolve().parent.parent / "data" / "revenue_bands.csv"
    return fallback.read_text(encoding="utf-8")


def default_revenue_bands() -> list[RevenueBand]:
    """Bands packaged with the code, used when no custom file exists."""
    return parse_revenue_bands_text(_packaged_bands_text())


def resolve_revenue_bands(
    data_dir: str | Path,
) -> tuple[list[RevenueBand], bool]:
    """Load bands for a data directory.

    Returns ``(bands, is_custom)``: a ``revenue_bands.csv`` under
    ``<data_dir>/reference/`` takes precedence; otherwise the packaged
    defaults are returned.
    """
    custom_path = revenue_bands_path(data_dir)
    if custom_path.is_file():
        return load_bands(custom_path), True
    return default_revenue_bands(), False


def load_revenue_bands(data_dir: str | Path) -> list[RevenueBand]:
    """Load bands for a data directory, preferring a custom file."""
    bands, _is_custom = resolve_revenue_bands(data_dir)
    return bands


def save_revenue_bands(data_dir: str | Path, bands: list[RevenueBand]) -> Path:
    """Write bands as the data directory's custom file. Returns its path."""
    path = revenue_bands_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(bands_to_csv_text(bands), encoding="utf-8")
    return path


def delete_custom_revenue_bands(data_dir: str | Path) -> bool:
    """Remove a custom bands file, restoring packaged defaults. Returns True if removed."""
    path = revenue_bands_path(data_dir)
    if path.is_file():
        path.unlink()
        return True
    return False


def estimate(employee_count: int | None, bands: list[RevenueBand]) -> float | None:
    if employee_count is None or employee_count <= 0:
        return None
    for band in bands:
        if band.min_employees <= employee_count <= band.max_employees:
            return band.midpoint_gbp
    return None


