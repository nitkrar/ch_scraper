"""Download published CQC bulk files from cqc.org.uk.

The CQC publishes weekly CSV snapshots at URLs of the shape:
    https://www.cqc.org.uk/system/files/YYYY-MM/DD_Month_YYYY_CQC_directory.csv

The files root has moved before (it was /sites/default/files/ until 2026), so
both spellings are accepted and links may be absolute or site-relative.

There's no stable "latest" alias — we scrape the listing page to find
the current direct link, then download to <data_dir>/input/cqc/.

The filename includes the publish date, which we extract and use as
the scrape_date for the upsert pipeline (analogous to the YYYY-MM-DD
prefix on CH BasicCompanyData files).
"""

from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path
from typing import Pattern

import httpx

from ch_bulk.core.paths import cqc_input_dir

logger = logging.getLogger(__name__)

CQC_BASE_URL = "https://www.cqc.org.uk"
CQC_LISTING_URL = "https://www.cqc.org.uk/about-us/transparency/using-cqc-data"

# CQC's edge blocks httpx's default "python-httpx/x.y" user-agent with a bare
# 403 on both the listing page and the asset URLs. Any conventional UA is let
# through, so send one rather than leaving the downloader dead.
_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    )
}

# Drupal serves published assets from a files root that CQC has changed once
# already (/sites/default/files/ -> /system/files/). Accept either rather than
# pinning to whichever is current.
_FILES_ROOT = r"/(?:sites/default/files|system/files)"
# Used only to make a failed match diagnosable.
_ANY_ASSET_RE = re.compile(_FILES_ROOT + r"/[^\"'\s)>]+", re.IGNORECASE)


def _asset_re(suffix: str) -> Pattern[str]:
    """Build a matcher for a dated CQC asset, e.g. ``12_August_2026_CQC_directory.csv``."""
    return re.compile(
        _FILES_ROOT + r"/(\d{4}-\d{2})/(\d{1,2})_([A-Za-z]+)_(\d{4})_" + suffix,
        re.IGNORECASE,
    )


CQC_FILENAME_RE = _asset_re(r"CQC_directory\.csv")
HSCA_FILENAME_RE = _asset_re(r"HSCA_Active_Locations\.ods")
_MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}


def _parse_cqc_date(day: str, month_name: str, year: str) -> date:
    return date(int(year), _MONTH_NAMES[month_name.lower()], int(day))


def _find_latest_asset_url(
    filename_re: Pattern[str],
    label: str,
    target_date: date | None = None,
) -> tuple[str, date]:
    """Scrape the CQC listing page for the latest matching bulk asset."""
    logger.info("Fetching CQC listing page: %s", CQC_LISTING_URL)
    resp = httpx.get(
        CQC_LISTING_URL, timeout=60, follow_redirects=True, headers=_HTTP_HEADERS
    )
    resp.raise_for_status()

    matches = list(filename_re.finditer(resp.text))
    if not matches:
        # Report what the page actually offered. The failure mode here is CQC
        # renaming or relocating assets, and naming the links we did see turns
        # a guessing game into a one-line diff of the expected pattern.
        seen = sorted({m.group(0) for m in _ANY_ASSET_RE.finditer(resp.text)})
        detail = (
            "Asset links present on the page:\n  " + "\n  ".join(seen[:15])
            if seen
            else "No asset links were found on the page at all."
        )
        raise RuntimeError(
            f"No {label} link found on {CQC_LISTING_URL}. "
            f"Has the CQC page layout changed?\n{detail}"
        )

    candidates: list[tuple[date, str]] = []
    for m in matches:
        try:
            published = _parse_cqc_date(m.group(2), m.group(3), m.group(4))
        except (KeyError, ValueError):
            continue
        candidates.append((published, f"https://www.cqc.org.uk{m.group(0)}"))

    if not candidates:
        raise RuntimeError(
            f"Found {label} link candidates on {CQC_LISTING_URL} "
            "but none had a parseable date."
        )

    if target_date is not None:
        for published, url in candidates:
            if published == target_date:
                return url, published
        raise RuntimeError(
            f"No {label} link found for {target_date.isoformat()} "
            f"on {CQC_LISTING_URL}."
        )

    published, url = max(candidates, key=lambda item: item[0])
    return url, published


def _download_asset(
    target_dir: Path,
    target_name: str,
    url_lookup: callable[[], tuple[str, date]],
    progress_callback: callable | None = None,
) -> Path:
    """Download a versioned CQC asset into ``target_dir``."""

    def _notify(msg: str) -> None:
        if progress_callback:
            progress_callback(msg)
        logger.info(msg)

    target_dir.mkdir(parents=True, exist_ok=True)
    url, scrape_date = url_lookup()
    target = target_dir / target_name.format(scrape_date=scrape_date.isoformat())

    if target.exists():
        _notify(
            f"{target.name} already on disk for "
            f"{scrape_date.isoformat()}: {target.name}"
        )
        return target

    _notify(f"Downloading {url} → {target.name}...")
    with httpx.stream(
        "GET", url, timeout=120, follow_redirects=True, headers=_HTTP_HEADERS
    ) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        written = 0
        last_pct = -1
        with target.open("wb") as f:
            for chunk in r.iter_bytes(chunk_size=64 * 1024):
                f.write(chunk)
                written += len(chunk)
                if total:
                    pct = (written * 100) // total
                    if pct != last_pct and pct % 10 == 0:
                        _notify(f"  download {pct}%")
                        last_pct = pct
    _notify(f"Downloaded {written:,} bytes to {target}")
    return target


def find_latest_csv_url() -> tuple[str, date]:
    """Scrape the CQC listing page for the most recent care directory CSV.

    Returns ``(absolute_url, scrape_date)``. Raises ``RuntimeError`` if
    no matching link is found.
    """
    return _find_latest_asset_url(CQC_FILENAME_RE, "CQC_directory.csv")


def find_latest_hsca_url(
    target_date: date | None = None,
) -> tuple[str, date]:
    """Scrape the CQC listing page for the most recent HSCA ODS file."""
    return _find_latest_asset_url(
        HSCA_FILENAME_RE,
        "HSCA_Active_Locations.ods",
        target_date=target_date,
    )


def download_cqc_directory(
    data_dir: str | Path,
    progress_callback: callable | None = None,
) -> Path:
    """Download the latest CQC care directory CSV to ``<data_dir>/input/cqc/``.

    The destination filename includes the scrape date for traceability
    and so the upsert pipeline can extract it: ``cqc_directory_YYYY-MM-DD.csv``.

    Idempotent on the date: if the file already exists with today's
    latest date, skip the download.

    Returns the path to the downloaded file.
    """
    target_dir = cqc_input_dir(data_dir)

    def _lookup() -> tuple[str, date]:
        if progress_callback:
            progress_callback("Looking up latest CQC directory URL...")
        return find_latest_csv_url()

    return _download_asset(
        target_dir=target_dir,
        target_name="cqc_directory_{scrape_date}.csv",
        url_lookup=_lookup,
        progress_callback=progress_callback,
    )


def download_hsca_filters(
    data_dir: str | Path,
    target_date: date | None = None,
    progress_callback: callable | None = None,
) -> Path:
    """Download the latest HSCA active locations ODS into ``data/input/cqc``.

    If ``target_date`` is supplied, fetch that exact published date from
    the listing page instead of the latest one.
    """
    target_dir = cqc_input_dir(data_dir)

    def _lookup() -> tuple[str, date]:
        if progress_callback:
            progress_callback("Looking up latest HSCA filters URL...")
        return find_latest_hsca_url(target_date=target_date)

    return _download_asset(
        target_dir=target_dir,
        target_name="hsca_active_locations_{scrape_date}.ods",
        url_lookup=_lookup,
        progress_callback=progress_callback,
    )
