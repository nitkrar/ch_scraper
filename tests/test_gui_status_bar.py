"""Searches report in their own pane, not the shared task status bar.

Finishing a task refreshes every pane and the search panes re-query, so a
search that wrote to the status bar would replace the task's final message.
These tests drive the pane methods with stand-in widgets: no Tk window opens.
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from ch_bulk import gui

PANES = {
    "CH": (gui.CHPane, "Companies"),
    "CQC": (gui.CQCPane, "Locations"),
    "Targets": (gui.TargetsPane, None),
}


class _Label:
    def __init__(self):
        self.text = ""

    def configure(self, **kwargs):
        self.text = kwargs.get("text", self.text)


def _pane(pane_class, sub_view, query):
    status_calls: list[str] = []
    app = SimpleNamespace(
        _task_running=False,
        _set_status=lambda msg, error=False: status_calls.append(msg),
        root=SimpleNamespace(after=lambda _ms, fn: fn()),
        _register_task=lambda worker, **_kwargs: worker(None),
    )
    pane = SimpleNamespace(
        app=app,
        ch=Mock(**{name: Mock(side_effect=query) for name in (
            "query_advanced",
            "query_cqc_locations_advanced",
            "query_tiered_targets_advanced",
        )}),
        sub_view=sub_view,
        btn_search=Mock(),
        btn_export=Mock(),
        count_label=_Label(),
        _get_filters=lambda: {},
        _display=lambda rows, total: pane.count_label.configure(text=f"{total} rows"),
    )
    return pane, status_calls


class SearchStatusTests(unittest.TestCase):
    def _run(self, query):
        results = {}
        for name, (pane_class, sub_view) in PANES.items():
            pane, status_calls = _pane(pane_class, sub_view, query)
            pane_class._run_query(pane)
            results[name] = (pane.count_label.text, status_calls)
        return results

    def test_successful_search_leaves_the_status_bar_alone(self):
        for name, (label, status_calls) in self._run(lambda **_f: ([], 7)).items():
            with self.subTest(pane=name):
                self.assertEqual(status_calls, [])
                self.assertEqual(label, "7 rows")

    def test_missing_database_is_reported_in_the_pane(self):
        def missing(**_filters):
            raise FileNotFoundError("no db")

        for name, (label, status_calls) in self._run(missing).items():
            with self.subTest(pane=name):
                self.assertEqual(status_calls, [])
                self.assertIn("Sync", label)

    def test_query_error_is_reported_in_the_pane(self):
        def broken(**_filters):
            raise RuntimeError("bad column")

        for name, (label, status_calls) in self._run(broken).items():
            with self.subTest(pane=name):
                self.assertEqual(status_calls, [])
                self.assertIn("bad column", label)


class ProgressFractionTests(unittest.TestCase):
    def test_counts_in_the_message_drive_the_bar(self):
        cases = {
            "Fetching directors 439/855 (51%) · about 6 min left": 439 / 855,
            "Fetching accounts 1,000/20,000 (5%)": 0.05,
            "Downloading part 3/7: BasicCompanyData-2026-05-01-part3_7.zip": 3 / 7,
        }
        for message, fraction in cases.items():
            with self.subTest(message=message):
                self.assertAlmostEqual(gui._progress_fraction(message), fraction)

    def test_messages_without_counts_keep_the_bar_animated(self):
        for message in ("Reading file", "Writing workbook", "CH syncing...", "Part 0/0",
                        "Saved to /exports/2026/11/out.xlsx"):
            with self.subTest(message=message):
                self.assertIsNone(gui._progress_fraction(message))


class StopButtonTests(unittest.TestCase):
    def _app(self, *, can_cancel=True):
        import threading
        event = threading.Event() if can_cancel else None
        task = SimpleNamespace(cancel_event=event)
        messages: list[str] = []
        app = SimpleNamespace(
            _lock=threading.Lock(),
            _registry_lock=threading.Lock(),
            _active_tasks={"t1": task},
            _current_task_id="t1",
            _task_running=True,
            btn_stop=Mock(),
            _set_task_msg=messages.append,
        )
        return app, event, messages

    def test_stop_signals_the_running_task(self):
        app, event, messages = self._app()
        gui.ChBulkApp._on_stop(app)
        self.assertTrue(event.is_set())
        self.assertTrue(messages and messages[-1].startswith("Stopping"))
        app.btn_stop.configure.assert_called_with(state="disabled")

    def test_stop_with_no_task_running_does_nothing(self):
        app, event, messages = self._app()
        app._current_task_id = None
        gui.ChBulkApp._on_stop(app)
        self.assertFalse(event.is_set())
        self.assertEqual(messages, [])


if __name__ == "__main__":
    unittest.main()
