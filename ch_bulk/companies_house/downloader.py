"""Download UK Companies House bulk CSV data (7-part ZIPs)."""

from __future__ import annotations

import logging
import zipfile
from datetime import date, timedelta
from pathlib import Path

import httpx
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://download.companieshouse.gov.uk"
PARTS = 7
CHUNK_SIZE = 256 * 1024  # 256 KB
_MAX_DOWNLOAD_RETRIES = 2


def _zip_filename(month: str, part: int) -> str:
    """Return the ZIP filename for a given month and part number.

    Args:
        month: Month string in ``YYYY-MM`` format.
        part: Part number (1-based).

    Returns:
        Filename like ``BasicCompanyData-2026-04-01-part1_7.zip``.
    """
    return f"BasicCompanyData-{month}-01-part{part}_{PARTS}.zip"


def _detect_month() -> str:
    """Auto-detect the latest available bulk data month.

    Tries the current month first, then falls back to the previous month
    by issuing HTTP HEAD requests.

    Returns:
        Month string in ``YYYY-MM`` format.

    Raises:
        RuntimeError: If neither month is available.
    """
    today = date.today()
    candidates = [
        today.strftime("%Y-%m"),
        (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m"),
    ]

    with httpx.Client(timeout=30, follow_redirects=True) as client:
        for month in candidates:
            url = f"{BASE_URL}/{_zip_filename(month, 1)}"
            try:
                resp = client.head(url)
                if resp.status_code == 200:
                    logger.info("Detected latest bulk data month: %s", month)
                    return month
            except httpx.HTTPError:
                continue

    raise RuntimeError(
        "Could not detect latest Companies House bulk data month. "
        f"Tried: {candidates}"
    )


def _download_file(
    client: httpx.Client,
    url: str,
    dest: Path,
    progress: Progress | None = None,
    progress_callback: callable | None = None,
) -> None:
    """Download a single file with resume support.

    Uses either a rich Progress bar (CLI) or a callback (GUI) for
    progress reporting. Exactly one should be provided.

    Args:
        client: httpx Client instance.
        url: URL to download.
        dest: Local destination path.
        progress: Rich Progress instance for CLI display.
        progress_callback: Callable for GUI progress updates.
    """
    headers: dict[str, str] = {}
    existing_size = 0

    if dest.exists():
        existing_size = dest.stat().st_size
        head = client.head(url)
        head.raise_for_status()
        remote_size = int(head.headers.get("content-length", 0))

        if existing_size == remote_size and remote_size > 0:
            logger.info("Already downloaded: %s", dest.name)
            return
        elif existing_size < remote_size:
            headers["Range"] = f"bytes={existing_size}-"
            logger.info(
                "Resuming %s from %d bytes", dest.name, existing_size
            )
        else:
            existing_size = 0

    # Set up progress tracking
    task_id = None
    if progress is not None:
        task_id = progress.add_task(dest.name, start=False)

    with client.stream("GET", url, headers=headers) as resp:
        if resp.status_code == 416:
            if progress is not None and task_id is not None:
                progress.update(task_id, visible=False)
            return

        resp.raise_for_status()

        total = int(resp.headers.get("content-length", 0))
        if existing_size and resp.status_code == 206:
            total += existing_size

        if progress is not None and task_id is not None:
            progress.update(task_id, total=total, completed=existing_size)
            progress.start_task(task_id)

        downloaded = existing_size
        mode = "ab" if resp.status_code == 206 else "wb"
        with open(dest, mode) as f:
            for chunk in resp.iter_bytes(chunk_size=CHUNK_SIZE):
                f.write(chunk)
                downloaded += len(chunk)
                if progress is not None and task_id is not None:
                    progress.advance(task_id, len(chunk))
                elif progress_callback and total > 0:
                    pct = int(downloaded * 100 / total)
                    progress_callback(
                        f"Downloading {dest.name}: {pct}% "
                        f"({downloaded // 1024 // 1024}MB / {total // 1024 // 1024}MB)"
                    )

    # Verify final size
    final_size = dest.stat().st_size
    if total and final_size != total:
        logger.warning(
            "Size mismatch for %s: expected %d, got %d",
            dest.name,
            total,
            final_size,
        )


def _extract_zip(zip_path: Path, extract_dir: Path) -> list[Path]:
    """Extract a ZIP file and return the list of extracted CSV paths.

    Validates each member path to prevent Zip Slip (path traversal)
    attacks. Entries that would escape ``extract_dir`` are skipped with
    a warning.

    On ``BadZipFile``, any files already extracted from the corrupt
    archive are deleted and an empty list is returned.

    Args:
        zip_path: Path to the ZIP file.
        extract_dir: Directory to extract into.

    Returns:
        List of paths to extracted CSV files (empty if the ZIP was
        corrupt).
    """
    csv_files: list[Path] = []
    resolved_dir = extract_dir.resolve()
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            for name in zf.namelist():
                if name.lower().endswith(".csv"):
                    # Zip Slip protection: verify the resolved target
                    # stays inside extract_dir.
                    target = (extract_dir / name).resolve()
                    if not (
                        str(target).startswith(str(resolved_dir) + "/")
                        or target == resolved_dir
                    ):
                        logger.warning(
                            "Skipping path-traversal entry in %s: %s",
                            zip_path.name,
                            name,
                        )
                        continue
                    zf.extract(name, extract_dir)
                    csv_files.append(extract_dir / name)
                    logger.info("Extracted: %s", name)
    except zipfile.BadZipFile:
        logger.warning(
            "Corrupt ZIP %s — deleting %d partially-extracted file(s)",
            zip_path.name,
            len(csv_files),
        )
        for partial in csv_files:
            if partial.exists():
                partial.unlink()
                logger.info("Deleted partial file: %s", partial.name)
        return []
    return csv_files


def download_bulk_data(
    data_dir: str | Path,
    month: str | None = None,
    keep_zips: bool = False,
    strict: bool = True,
    progress_callback: callable | None = None,
) -> list[Path]:
    """Download Companies House bulk CSV data (7-part ZIPs).

    Downloads all 7 parts, extracts CSV files, and optionally removes
    the ZIPs afterwards.

    Args:
        data_dir: Directory to store downloaded/extracted files.
        month: Month in ``YYYY-MM`` format, or ``None`` to auto-detect.
        keep_zips: If ``True``, keep ZIP files after extraction.
        strict: If ``True`` (default), raise on corrupt ZIPs instead
            of silently skipping them.
        progress_callback: If provided, called with a status string
            instead of showing rich Progress bars. Use this when
            calling from a GUI to route progress to the UI.

    Returns:
        List of paths to extracted CSV files.

    Raises:
        RuntimeError: If the month cannot be detected.
        httpx.HTTPError: On download failures.
        zipfile.BadZipFile: If ``strict=True`` and a ZIP is corrupt.
    """
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    if month is None:
        if progress_callback:
            progress_callback("Detecting latest data month...")
        month = _detect_month()

    logger.info("Downloading Companies House bulk data for %s", month)

    zip_paths: list[Path] = []
    all_csv_files: list[Path] = []

    with httpx.Client(
        timeout=httpx.Timeout(connect=30, read=600, write=30, pool=30),
        follow_redirects=True,
    ) as client:
        if progress_callback:
            # GUI path: use callback for progress, no rich output
            for part in range(1, PARTS + 1):
                filename = _zip_filename(month, part)
                url = f"{BASE_URL}/{filename}"
                dest = data_dir / filename

                progress_callback(f"Downloading part {part}/{PARTS}: {filename}")

                last_exc: httpx.HTTPError | None = None
                for attempt in range(1, _MAX_DOWNLOAD_RETRIES + 1):
                    try:
                        _download_file(
                            client, url, dest,
                            progress_callback=progress_callback,
                        )
                        zip_paths.append(dest)
                        last_exc = None
                        break
                    except httpx.HTTPError as exc:
                        last_exc = exc
                        progress_callback(
                            f"Retry {attempt}/{_MAX_DOWNLOAD_RETRIES} for {filename}..."
                        )
                if last_exc is not None:
                    raise last_exc
        else:
            # CLI path: use rich Progress bars in terminal
            progress = Progress(
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
            )
            with progress:
                for part in range(1, PARTS + 1):
                    filename = _zip_filename(month, part)
                    url = f"{BASE_URL}/{filename}"
                    dest = data_dir / filename

                    last_exc = None
                    for attempt in range(1, _MAX_DOWNLOAD_RETRIES + 1):
                        try:
                            _download_file(client, url, dest, progress)
                            zip_paths.append(dest)
                            last_exc = None
                            break
                        except httpx.HTTPError as exc:
                            last_exc = exc
                            logger.warning(
                                "Attempt %d/%d failed for %s: %s",
                                attempt,
                                _MAX_DOWNLOAD_RETRIES,
                                filename,
                                exc,
                            )
                    if last_exc is not None:
                        logger.error(
                            "Failed to download %s after %d attempts",
                            filename,
                            _MAX_DOWNLOAD_RETRIES,
                        )
                        raise last_exc

    # Extract all ZIPs
    if progress_callback:
        progress_callback("Extracting ZIP files...")
    logger.info("Extracting ZIP files...")
    for zip_path in zip_paths:
        csv_files = _extract_zip(zip_path, data_dir)
        if not csv_files and strict:
            raise zipfile.BadZipFile(
                f"Corrupt or empty ZIP file: {zip_path.name}. "
                f"Use strict=False to skip corrupt ZIPs."
            )
        all_csv_files.extend(csv_files)

    # Clean up ZIPs if requested
    if not keep_zips:
        for zip_path in zip_paths:
            if zip_path.exists():
                zip_path.unlink()
                logger.info("Removed: %s", zip_path.name)

    logger.info(
        "Download complete: %d CSV files extracted", len(all_csv_files)
    )
    return all_csv_files
