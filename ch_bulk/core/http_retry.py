"""Shared `Retry-After` parsing for HTTP clients.

`Retry-After` is either delay-seconds or an HTTP-date. Clients used to
parse it with `int()` only, so a date value silently fell back to the
default pause. Pauses are capped so a large server-supplied value
cannot stall a run for an hour.
"""

from __future__ import annotations

import email.utils
from datetime import datetime, timezone

DEFAULT_RETRY_AFTER_SECONDS = 60.0
MAX_RETRY_AFTER_SECONDS = 300.0


def parse_retry_after(
    value: object,
    *,
    default_seconds: float = DEFAULT_RETRY_AFTER_SECONDS,
    max_seconds: float = MAX_RETRY_AFTER_SECONDS,
    now: datetime | None = None,
) -> float:
    """Parse a `Retry-After` header value (or headers mapping) to seconds.

    Returns `default_seconds` for missing, empty, or unparseable
    values, clamped to ``[0, max_seconds]`` otherwise.
    """
    if value is not None and hasattr(value, "get"):
        mapping = value  # type: ignore[attr-defined]
        raw = mapping.get("Retry-After")
        if raw is None:
            raw = mapping.get("retry-after")
        value = raw
    if value is None:
        return default_seconds
    text = str(value).strip()
    if not text:
        return default_seconds
    try:
        return max(0.0, min(float(int(text)), max_seconds))
    except ValueError:
        pass
    try:
        parsed = email.utils.parsedate_to_datetime(text)
    except (TypeError, ValueError, OverflowError):
        return default_seconds
    if parsed is None:
        return default_seconds
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    reference = now if now is not None else datetime.now(timezone.utc)
    delta = (parsed - reference).total_seconds()
    return max(0.0, min(delta, max_seconds))
