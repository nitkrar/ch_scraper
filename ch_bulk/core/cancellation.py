"""Cooperative cancellation primitives for long-running pipelines."""

from __future__ import annotations

import time
from collections.abc import Callable
from threading import Event


class OperationCancelled(RuntimeError):
    """Raised when a cooperative pipeline detects cancellation."""


def is_cancelled(cancel_event: Event | None) -> bool:
    return cancel_event is not None and cancel_event.is_set()


def raise_if_cancelled(
    cancel_event: Event | None,
    *,
    reason: str = "operation cancelled",
) -> None:
    if is_cancelled(cancel_event):
        raise OperationCancelled(reason)


def cancellable_sleep(
    cancel_event: Event | None,
    seconds: float,
    *,
    reason: str = "operation cancelled",
    sleep_fn: Callable[[float], None] = time.sleep,
) -> None:
    """Sleep up to `seconds`, raising if the cancel event fires."""
    seconds = max(seconds, 0.0)
    if cancel_event is None:
        sleep_fn(seconds)
        return
    if cancel_event.wait(timeout=seconds):
        raise OperationCancelled(reason)
