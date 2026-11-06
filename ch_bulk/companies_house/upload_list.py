"""Read a user-supplied CSV/Excel list of Companies House numbers."""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path

from openpyxl import load_workbook

HEADER_KEYS = (
    "companynumber",
    "companyno",
    "companiesnumber",
    "companieshousenumber",
    "companieshouseno",
    "chnumber",
    "chno",
    "crn",
    "companyregistrationnumber",
    "registrationnumber",
    "regno",
    "registerednumber",
)
COMPANY_NUMBER_LENGTH = 8
# Exported reports put titles and notes above the table, so the header row
# is searched for this far down.
HEADER_SEARCH_ROWS = 10

_DIGITS_RE = re.compile(r"^\d{1,8}$")
_PREFIXED_RE = re.compile(r"^([A-Z]{1,2})(\d+)$")
_HEADER_STRIP_RE = re.compile(r"[\s_\-.#]")


@dataclass(frozen=True)
class UploadInput:
    numbers: list[str]
    invalid: list[str]
    column: str | None


def normalise_company_number(raw: object) -> str | None:
    """Return the canonical 8-character company number, or None if invalid.

    Excel stores numbers like ``01234567`` as numeric cells, dropping the
    leading zero, so ints and whole floats are re-padded.
    """
    if raw is None:
        return None
    if isinstance(raw, float):
        if not raw.is_integer():
            return None
        raw = int(raw)
    text = "".join(str(raw).split()).upper()
    if not text:
        return None
    if _DIGITS_RE.match(text):
        return text.zfill(COMPANY_NUMBER_LENGTH)
    match = _PREFIXED_RE.match(text)
    if match and len(text) <= COMPANY_NUMBER_LENGTH:
        prefix, digits = match.groups()
        return prefix + digits.zfill(COMPANY_NUMBER_LENGTH - len(prefix))
    return None


def _cell_text(value: object) -> str:
    return "" if value is None else str(value).strip()


def _header_key(value: object) -> str:
    return _HEADER_STRIP_RE.sub("", _cell_text(value).lower())


def _read_rows(path: Path) -> list[list[object]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open(newline="", encoding="utf-8-sig") as handle:
            return [list(row) for row in csv.reader(handle)]
    if suffix == ".xlsx":
        workbook = load_workbook(path, read_only=True, data_only=True)
        try:
            sheet = workbook.worksheets[0]
            return [list(row) for row in sheet.iter_rows(values_only=True)]
        finally:
            workbook.close()
    raise ValueError(f"Unsupported file type {path.suffix!r}: use .csv or .xlsx")


def _cell(row: list[object], index: int) -> object:
    return row[index] if index < len(row) else None


def _detect_column(rows: list[list[object]]) -> tuple[int, str | None, int]:
    """Return (column index, header text, index of the first data row)."""
    for row_index, row in enumerate(rows[:HEADER_SEARCH_ROWS]):
        for index, value in enumerate(row):
            if _header_key(value) in HEADER_KEYS:
                return index, _cell_text(value), row_index + 1

    first = rows[0] if rows else []

    first_non_empty = next((v for v in first if _cell_text(v)), None)
    if normalise_company_number(first_non_empty) is not None:
        for index, value in enumerate(first):
            if normalise_company_number(value) is not None:
                return index, None, 0

    width = max((len(row) for row in rows), default=0)
    for index in range(width):
        non_empty = 0
        valid = 0
        for row in rows[1:]:
            value = _cell(row, index)
            if not _cell_text(value):
                continue
            non_empty += 1
            if normalise_company_number(value) is not None:
                valid += 1
        if non_empty and valid * 2 >= non_empty:
            return index, _cell_text(_cell(first, index)), 1

    raise ValueError("No company-number column was found in the file")


def read_company_numbers(path: str | Path) -> UploadInput:
    rows = _read_rows(Path(path))
    index, column, data_start = _detect_column(rows)

    numbers: list[str] = []
    invalid: list[str] = []
    seen_numbers: set[str] = set()
    seen_invalid: set[str] = set()
    for row in rows[data_start:]:
        value = _cell(row, index)
        text = _cell_text(value)
        if not text:
            continue
        number = normalise_company_number(value)
        if number is None:
            if text not in seen_invalid:
                seen_invalid.add(text)
                invalid.append(text)
        elif number not in seen_numbers:
            seen_numbers.add(number)
            numbers.append(number)
    return UploadInput(numbers=numbers, invalid=invalid, column=column)
