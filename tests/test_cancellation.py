"""Regression tests for cooperative cancellation primitives."""

from __future__ import annotations

import time
import unittest
from threading import Event, Thread

from ch_bulk.core.cancellation import (
    OperationCancelled,
    cancellable_sleep,
    is_cancelled,
    raise_if_cancelled,
)


class CancellationPrimitiveTests(unittest.TestCase):
    def test_is_cancelled_none(self) -> None:
        self.assertFalse(is_cancelled(None))

    def test_is_cancelled_set(self) -> None:
        cancel_event = Event()
        self.assertFalse(is_cancelled(cancel_event))
        cancel_event.set()
        self.assertTrue(is_cancelled(cancel_event))

    def test_raise_if_cancelled(self) -> None:
        cancel_event = Event()
        raise_if_cancelled(cancel_event)
        cancel_event.set()
        with self.assertRaises(OperationCancelled):
            raise_if_cancelled(cancel_event)

    def test_cancellable_sleep_none(self) -> None:
        started = time.monotonic()
        cancellable_sleep(None, 0.05)
        self.assertGreater(time.monotonic() - started, 0.02)

    def test_cancellable_sleep_already_cancelled(self) -> None:
        cancel_event = Event()
        cancel_event.set()
        with self.assertRaises(OperationCancelled):
            cancellable_sleep(cancel_event, 1.0)

    def test_cancellable_sleep_cancelled_mid_sleep(self) -> None:
        cancel_event = Event()
        setter = Thread(
            target=lambda: (time.sleep(0.05), cancel_event.set()),
            daemon=True,
        )
        setter.start()
        with self.assertRaises(OperationCancelled):
            cancellable_sleep(cancel_event, 1.0)
        setter.join(1.0)
        self.assertFalse(setter.is_alive())


if __name__ == "__main__":
    unittest.main()
