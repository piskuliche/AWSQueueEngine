"""Every queue-host state file writes atomically and complains when unreadable.

The five state files are written by five near-identical modules, so rather than
repeat the same two cases in five per-module test files, the pair is asserted
once here against a table. The per-module files keep their own behavioural
tests; this covers only the shared durability contract.
"""
import io
import unittest
from contextlib import redirect_stderr
from pathlib import Path
import tempfile

from awsqueueengine.host import monitor
from awsqueueengine.shared import (
    completion_state,
    deferred_state,
    failure_state,
    queue,
    running_state,
)


class Unserializable:
    """json.dumps raises TypeError on this; nothing else about it matters."""


#: (label, module, path-attribute, save, load, empty, good value, unserializable value)
STATE_MODULES = [
    (
        "queue",
        queue,
        "QUEUE_FILE",
        lambda value: queue.save_queue(value),
        lambda: queue.load_queue(),
        [],
        [{"cmd": "keep-me"}],
        [Unserializable()],
    ),
    (
        "running",
        running_state,
        "RUNNING_FILE",
        lambda value: running_state.save_running_jobs(value),
        lambda: running_state.load_running_jobs(),
        {},
        {"eci1": {"cmd": "keep-me"}},
        # `cmd` survives normalize_job_item untouched, so this reaches json.dumps.
        {"eci1": {"cmd": Unserializable()}},
    ),
    (
        "completed",
        completion_state,
        "COMPLETED_FILE",
        lambda value: completion_state.save_completed_jobs(value),
        lambda: completion_state.load_completed_jobs(),
        [],
        [{"host": "eci1"}],
        [Unserializable()],
    ),
    (
        "failed",
        failure_state,
        "FAILED_FILE",
        lambda value: failure_state.save_failed_jobs(value),
        lambda: failure_state.load_failed_jobs(),
        [],
        [{"host": "eci1"}],
        [Unserializable()],
    ),
    (
        "deferred",
        deferred_state,
        "DEFERRED_FILE",
        lambda value: deferred_state.save_deferred_jobs(value),
        lambda: deferred_state.load_deferred_jobs(),
        [],
        [{"cmd": "keep-me"}],
        [Unserializable()],
    ),
]


class StateFileAtomicityTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _redirect(self, module, attribute, name):
        """Point one state module at a file in this test's tempdir."""
        path = self.root / name
        original = getattr(module, attribute)
        setattr(module, attribute, path)
        self.addCleanup(setattr, module, attribute, original)
        return path

    def _litter(self):
        return sorted(p.name for p in self.root.iterdir() if ".tmp." in p.name)

    def test_unreadable_file_reads_as_empty_and_warns(self):
        for label, module, attribute, _save, load, empty, *_ in STATE_MODULES:
            with self.subTest(state=label):
                path = self._redirect(module, attribute, f"{label}.json")
                path.write_text("{ this is not json")

                stderr = io.StringIO()
                with redirect_stderr(stderr):
                    self.assertEqual(load(), empty)

                self.assertIn(str(path), stderr.getvalue())

    def test_failed_write_preserves_previous_content_and_leaves_no_litter(self):
        for label, module, attribute, save, load, _empty, good, bad in STATE_MODULES:
            with self.subTest(state=label):
                self._redirect(module, attribute, f"{label}.json")
                save(good)
                # Compare against what a read gives back rather than against
                # `good` itself: some of these modules normalize on load.
                before = load()
                self.assertTrue(before)

                with self.assertRaises(TypeError):
                    save(bad)

                self.assertEqual(load(), before)
                self.assertEqual(self._litter(), [])


class MonitorStateAtomicityTests(unittest.TestCase):
    """monitor_state.json is a sixth state file with the same contract."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.path = Path(self.tmpdir.name) / "monitor_state.json"
        self.original = monitor.MONITOR_STATE_FILE
        monitor.MONITOR_STATE_FILE = self.path

    def tearDown(self):
        monitor.MONITOR_STATE_FILE = self.original
        self.tmpdir.cleanup()

    def test_unreadable_file_reads_as_empty_and_warns(self):
        self.path.write_text("not json")
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            self.assertEqual(monitor._load_monitor_state(), {})
        self.assertIn(str(self.path), stderr.getvalue())

    def test_failed_write_preserves_previous_content(self):
        # _save_monitor_state swallows the error by design (a monitor must not
        # die over one bad write); what matters is that the old state survives.
        monitor._save_monitor_state({"migrated_at": 1.0})
        monitor._save_monitor_state({"bad": Unserializable()})
        self.assertEqual(monitor._load_monitor_state(), {"migrated_at": 1.0})


if __name__ == "__main__":
    unittest.main()
