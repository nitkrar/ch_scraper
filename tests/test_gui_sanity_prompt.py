"""The GUI's force prompt after a failed sanity check."""

from __future__ import annotations

import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from ch_bulk import gui
from ch_bulk.companies_house.processor import SanityCheckError, SanityCheckResult


def _row_count_failure() -> SanityCheckError:
    result = SanityCheckResult(
        companies_exists=True,
        dup_distinct_numbers=0,
        dup_excess_rows=0,
        dup_sample=[],
        old_total=6_157_412,
        new_total=5_711_779,
        row_delta=-445_633,
        row_pct=7.24,
        currently_active=5_711_779,
        would_be_inactivated=0,
        inactive_pct=0.0,
    )
    return SanityCheckError(result, "Sanity check FAILED")


class SanityPromptTests(unittest.TestCase):
    def test_force_prompt_runs_after_the_worker_has_returned(self):
        scheduled = []
        app = SimpleNamespace(
            _lock=threading.Lock(),
            root=SimpleNamespace(after=lambda _ms, fn: scheduled.append(fn)),
            _progress_cb=lambda _msg: None,
            _set_task_msg=lambda _msg: None,
            _set_task_error=lambda _msg: None,
            _task_done=lambda: None,
            _run_task=lambda worker, _label: worker(),
        )

        def work_fn(force, cancel_event=None):
            raise _row_count_failure()

        gui.ChBulkApp._dispatch_with_sanity(
            app, work_fn, success_msg=str, label="CH syncing..."
        )
        self.assertEqual(len(scheduled), 1)

        # Tk runs the callback later, after the except block has finished.
        with patch.object(gui.messagebox, "askyesno", return_value=False) as ask:
            scheduled[0]()

        body = ask.call_args.args[1]
        self.assertIn("Row count delta: 7.24%", body)


if __name__ == "__main__":
    unittest.main()
