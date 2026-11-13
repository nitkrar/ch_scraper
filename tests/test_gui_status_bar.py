"""Searches report in their own pane, not the shared task status bar.

Finishing a task refreshes every pane and the search panes re-query, so a
search that wrote to the status bar would replace the task's final message.
These tests drive the pane methods with stand-in widgets: no Tk window opens.
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from ch_bulk import gui

PANES = {
    "CH": (gui.CHPane, "Companies"),
    "CQC": (gui.CQCPane, "Locations"),
    "Targets": (gui.TargetsPane, "Targets"),
}


class _Label:
    def __init__(self):
        self.text = ""

    def configure(self, **kwargs):
        self.text = kwargs.get("text", self.text)


def _pane(pane_class, sub_view, ch=None):
    """A real pane instance with stand-in widgets: no Tk window opens."""
    status_calls: list[str] = []
    pane = pane_class.__new__(pane_class)
    pane.app = SimpleNamespace(
        _task_running=False,
        _set_status=lambda msg, error=False: status_calls.append(msg),
        root=SimpleNamespace(after=lambda _ms, fn: fn()),
        _register_task=lambda worker, **_kwargs: worker(None),
    )
    pane.ch = ch or Mock()
    pane.sub_view = sub_view
    pane.page = 1
    pane.total_pages = 1
    pane._reset_sort()
    pane.btn_search = Mock()
    pane.btn_export = Mock()
    pane.count_label = _Label()
    pane._get_filters = pane._paging_filters
    pane._display = lambda rows, total: pane.count_label.configure(text=f"{total} rows")
    return pane, status_calls


def _ch_returning(query):
    names = [spec[1] for pane_class, _ in PANES.values() for spec in pane_class.VIEWS.values()]
    return Mock(**{name: Mock(side_effect=query) for name in names})


class SearchStatusTests(unittest.TestCase):
    def _run(self, query):
        results = {}
        for name, (pane_class, sub_view) in PANES.items():
            pane, status_calls = _pane(pane_class, sub_view, _ch_returning(query))
            pane._run_query()
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


class BrowsePaneTests(unittest.TestCase):
    """Behaviour every search pane shares: one query per sub-view, sorting, export."""

    def _views(self):
        for pane_class, _ in PANES.values():
            for view, (sort_column, query_name, export_name) in pane_class.VIEWS.items():
                yield pane_class, view, sort_column, query_name, export_name

    def test_each_view_runs_its_own_query_with_paging(self):
        for pane_class, view, sort_column, query_name, _ in self._views():
            with self.subTest(pane=pane_class.__name__, view=view):
                ch = Mock()
                getattr(ch, query_name).return_value = ([], 0)
                pane, _ = _pane(pane_class, view, ch)
                pane.page = 3
                pane._run_query()
                getattr(ch, query_name).assert_called_once_with(
                    sort_by=sort_column,
                    sort_order=pane_class.default_sort_order,
                    page=3,
                    page_size=gui.PAGE_SIZE,
                )

    def test_sorting_toggles_on_the_same_column_and_resets_on_a_new_one(self):
        pane, _ = _pane(gui.CHPane, "Directors Age", _ch_returning(lambda **_f: ([], 0)))
        pane.page = 4
        pane._on_sort("company_name")
        self.assertEqual((pane.sort_by["Directors Age"], pane.sort_order), ("company_name", "DESC"))
        self.assertEqual(pane.page, 1)
        pane._on_sort("company_name")
        self.assertEqual(pane.sort_order, "ASC")
        pane._on_sort("max_director_age")
        self.assertEqual((pane.sort_by["Directors Age"], pane.sort_order), ("max_director_age", "ASC"))
        # Each sub-view keeps its own sort column.
        self.assertEqual(pane.sort_by["Companies"], "company_name")

    def test_targets_default_to_highest_score_first(self):
        pane, _ = _pane(gui.TargetsPane, "Targets")
        self.assertEqual((pane.sort_by["Targets"], pane.sort_order), ("total_score", "DESC"))
        pane._on_sort("total_score")
        self.assertEqual(pane.sort_order, "ASC")

    def test_export_uses_the_views_export_without_paging(self):
        for pane_class, view, _, _, export_name in self._views():
            with self.subTest(pane=pane_class.__name__, view=view):
                ch = Mock()
                getattr(ch, export_name).return_value = 5
                pane, status_calls = _pane(pane_class, view, ch)
                with patch.object(gui.filedialog, "asksaveasfilename", return_value="/tmp/x.csv") as ask:
                    pane._on_export()
                getattr(ch, export_name).assert_called_once_with("/tmp/x.csv")
                self.assertTrue(ask.call_args.kwargs["initialfile"].endswith("_export.csv"))
                self.assertEqual(
                    status_calls[-1], f"Exported 5 {pane_class.row_noun} to /tmp/x.csv"
                )


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
