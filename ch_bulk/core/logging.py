"""Shared logging setup for ch_bulk.

Installed at every entry point (GUI main, CLI main, ChBulk.__init__)
so per-phase timings from the @timed_phase decorator always land in
the same log file regardless of how the code is invoked.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from ch_bulk.core.paths import logs_dir

LOG_FILENAME = "ch-bulk.log"


def setup_logging(data_dir: str | Path, level: int = logging.INFO) -> Path:
    """Install a file handler for ``<data_dir>/logs/ch-bulk.log``.

    Idempotent: re-calling with the same path doesn't add duplicate
    handlers. Returns the log file path.
    """
    log_dir = logs_dir(data_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / LOG_FILENAME

    root = logging.getLogger()
    abs_target = str(log_path.resolve())

    # Skip if a handler for this exact path is already installed
    for h in root.handlers:
        if isinstance(h, logging.FileHandler) and h.baseFilename == abs_target:
            return log_path

    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    root.addHandler(handler)
    if root.level == logging.NOTSET or root.level > level:
        root.setLevel(level)

    # Silence noisy network libraries that would clutter the log
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    return log_path


class FsyncLineLogger:
    """Dedicated per-run progress log with line buffering + explicit fsync."""

    def __init__(
        self,
        data_dir: str | Path,
        *,
        sync_type: str,
        batch_id: str,
        filename_prefix: str | None = None,
    ) -> None:
        log_dir = logs_dir(data_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        if filename_prefix:
            self.path = log_dir / f"{filename_prefix}_{batch_id}.log"
        else:
            self.path = log_dir / f"enrich_{sync_type}_{batch_id}.log"
        self.buffering = 1
        self._handle = open(
            self.path,
            "a",
            encoding="utf-8",
            buffering=self.buffering,
        )

    def __enter__(self) -> "FsyncLineLogger":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def write_line(self, message: str) -> None:
        timestamp = (
            datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )
        self._handle.write(f"{timestamp} {message}\n")
        self._handle.flush()

    def flush_and_fsync(self) -> None:
        self._handle.flush()
        os.fsync(self._handle.fileno())

    def close(self) -> None:
        self._handle.close()
