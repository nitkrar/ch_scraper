from __future__ import annotations

import threading
import unittest
from unittest.mock import Mock, patch

from ch_bulk.core.rate_limit import SlidingWindowThrottle


class _FakeClock:
    def __init__(self) -> None:
        self._value = 0.0
        self._lock = threading.Lock()

    def monotonic(self) -> float:
        with self._lock:
            return self._value

    def advance(self, delta: float) -> None:
        with self._lock:
            self._value += delta


class SlidingWindowThrottleTests(unittest.TestCase):
    def test_wait_releases_lock_while_sleeping(self) -> None:
        throttle = SlidingWindowThrottle(
            1,
            300,
            label="test_throttle",
            logger=Mock(),
        )
        clock = _FakeClock()
        release_sleep = threading.Event()
        both_sleepers_entered = threading.Event()
        sleep_calls: list[float] = []
        sleep_calls_lock = threading.Lock()
        errors: list[BaseException] = []

        def fake_sleep(duration: float) -> None:
            with sleep_calls_lock:
                sleep_calls.append(duration)
                if len(sleep_calls) >= 2:
                    both_sleepers_entered.set()
            self.assertTrue(
                release_sleep.wait(1.0),
                "sleepers did not get released",
            )
            clock.advance(duration)

        def worker() -> None:
            try:
                throttle.wait()
            except BaseException as exc:  # pragma: no cover - assertion surface
                errors.append(exc)

        with (
            patch("ch_bulk.core.rate_limit.time.monotonic", side_effect=clock.monotonic),
            patch("ch_bulk.core.rate_limit.time.sleep", side_effect=fake_sleep),
        ):
            throttle.wait()  # consume the single available slot at t=0

            first = threading.Thread(target=worker)
            second = threading.Thread(target=worker)
            first.start()
            second.start()

            self.assertTrue(
                both_sleepers_entered.wait(0.5),
                f"expected both waiters to reach sleep; calls={sleep_calls!r}",
            )
            release_sleep.set()

            first.join(1.0)
            second.join(1.0)

        self.assertFalse(errors, f"worker errors: {errors!r}")
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())


if __name__ == "__main__":
    unittest.main()
