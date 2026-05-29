"""Shared sliding-window throttling utilities for external APIs."""

from __future__ import annotations

import logging
import threading
import time
from collections import deque

from ch_bulk.core.cancellation import cancellable_sleep


class SlidingWindowThrottle:
    """Simple sliding-window throttle.

    Keeps the last N request timestamps and sleeps when the next request
    would exceed the allowed request rate.
    """

    def __init__(
        self,
        max_requests: int,
        window_seconds: int,
        *,
        label: str,
        logger: logging.Logger | None = None,
    ) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.label = label
        self.logger = logger or logging.getLogger(__name__)
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def wait(self, *, cancel_event: threading.Event | None = None) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                while self._timestamps and now - self._timestamps[0] > self.window_seconds:
                    self._timestamps.popleft()

                if len(self._timestamps) < self.max_requests:
                    self._timestamps.append(time.monotonic())
                    return

                sleep_for = self.window_seconds - (now - self._timestamps[0]) + 0.01
                sleep_for = max(0.0, sleep_for)
                self.logger.info(
                    "Throttle %s hit: sleeping %.2fs",
                    self.label,
                    sleep_for,
                )
            cancellable_sleep(
                cancel_event,
                sleep_for,
                reason=f"throttle wait cancelled: {self.label}",
                sleep_fn=time.sleep,
            )
