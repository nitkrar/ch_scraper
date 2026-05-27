"""Download published CQC bulk files from cqc.org.uk.

The CQC publishes weekly CSV snapshots at URLs of the shape:
    https://www.cqc.org.uk/sites/default/files/YYYY-MM/DD_Month_YYYY_CQC_directory.csv

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

logger = logging.getLogger(__name__)

CQC_LISTING_URL = "https://www.cqc.org.uk/about-us/transparency/using-cqc-data"
CQC_FILENAME_RE = re.compile(
    r"/sites/default/files/(\d{4}-\d{2})/(\d{1,2})_([A-Za-z]+)_(\d{4})_CQC_directory\.csv",
    re.IGNORECASE,
)
HSCA_FILENAME_RE = re.compile(
    r"/sites/default/files/(\d{4}-\d{2})/(\d{1,2})_([A-Za-z]+)_(\d{4})_HSCA_Active_Locations\.ods",
    re.IGNORECASE,
)
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
    resp = httpx.get(CQC_LISTING_URL, timeout=60, follow_redirects=True)
    resp.raise_for_status()

    matches = list(filename_re.finditer(resp.text))
    if not matches:
        raise RuntimeError(
            f"No {label} link found on {CQC_LISTING_URL}. "
            "Has the CQC page layout changed?"
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
    with httpx.stream("GET", url, timeout=120, follow_redirects=True) as r:
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
    data_dir = Path(data_dir)
    target_dir = data_dir / "input" / "cqc"

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
    data_dir = Path(data_dir)
    target_dir = data_dir / "input" / "cqc"

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
