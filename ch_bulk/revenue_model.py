"""Deterministic employee-count to revenue estimate model."""

from __future__ import annotations

import csv
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RevenueBand:
    min_employees: int
    max_employees: int
    low_gbp: float
    high_gbp: float
    midpoint_gbp: float


def load_bands(csv_path: Path) -> list[RevenueBand]:
    bands: list[RevenueBand] = []
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            bands.append(
                RevenueBand(
                    min_employees=int(row["min_employees"]),
                    max_employees=(
                        int(row["max_employees"])
                        if row["max_employees"]
                        else sys.maxsize
                    ),
                    low_gbp=float(row["low_gbp"]),
                    high_gbp=float(row["high_gbp"]),
                    midpoint_gbp=float(row["midpoint_gbp"]),
                )
            )
    return bands


def estimate(employee_count: int | None, bands: list[RevenueBand]) -> float | None:
    if employee_count is None or employee_count <= 0:
        return None
    for band in bands:
        if band.min_employees <= employee_count <= band.max_employees:
            return band.midpoint_gbp
    return None
