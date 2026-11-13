"""Validation for pagination and limit arguments shared by query helpers.

Rejecting invalid values here, with ValueError, keeps DuckDB binder
errors out of API and GUI error paths.
"""

from __future__ import annotations

import operator


def _coerce_int(name: str, value: object) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer, got {value!r}")
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            raise ValueError(f"{name} must be an integer, got {value!r}") from None
    try:
        return operator.index(value)  # type: ignore[arg-type]
    except TypeError:
        raise ValueError(f"{name} must be an integer, got {value!r}") from None


def validate_limit(limit: int | None) -> int | None:
    """Return `limit`, rejecting negative values. None means no limit."""
    if limit is None:
        return None
    coerced = _coerce_int("limit", limit)
    if coerced < 0:
        raise ValueError(f"limit must be >= 0, got {limit!r}")
    return coerced


def validate_pagination(page: int, page_size: int) -> int:
    """Return the OFFSET for a 1-based page. Rejects page < 1 and page_size < 1."""
    coerced_page = _coerce_int("page", page)
    coerced_size = _coerce_int("page_size", page_size)
    if coerced_page < 1:
        raise ValueError(f"page must be >= 1, got {page!r}")
    if coerced_size < 1:
        raise ValueError(f"page_size must be >= 1, got {page_size!r}")
    return (coerced_page - 1) * coerced_size
