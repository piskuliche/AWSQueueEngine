import io
import multiprocessing
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

from awsqueueengine.shared import state_lock


def _hold_lock_in_child(lock_path, ready, release):
    """Take the lock in a separate process and hold it until told to let go."""
    state_lock.STATE_LOCK_FILE = Path(lock_path)
    with state_lock.state_lock():
        ready.set()
        release.wait(10)


class StateLockTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.lock_path = Path(self.tmpdir.name) / "state.lock"
        self.original = state_lock.STATE_LOCK_FILE
        state_lock.STATE_LOCK_FILE = self.lock_path

    def tearDown(self):
        state_lock.STATE_LOCK_FILE = self.original
        self.tmpdir.cleanup()
        self.assertEqual(state_lock._depth, 0, "a test leaked the state lock")

    def test_acquires_and_releases(self):
        self.assertEqual(state_lock._depth, 0)
        with state_lock.state_lock():
            self.assertEqual(state_lock._depth, 1)
        self.assertEqual(state_lock._depth, 0)
        self.assertIsNone(state_lock._handle)

    def test_creates_the_lock_file_and_its_parent(self):
        nested = Path(self.tmpdir.name) / "host" / "state.lock"
        state_lock.STATE_LOCK_FILE = nested
        with state_lock.state_lock():
            pass
        self.assertTrue(nested.exists())

    def test_nesting_does_not_deadlock(self):
        # flock is per open file description, so a naive second open() + LOCK_EX
        # in the same process would block on itself forever. Callers do nest:
        # requeue_deferred pops from the deferred list and then enqueues.
        with state_lock.state_lock():
            with state_lock.state_lock():
                with state_lock.state_lock():
                    self.assertEqual(state_lock._depth, 3)
                self.assertEqual(state_lock._depth, 2)
        self.assertEqual(state_lock._depth, 0)

    def test_inner_exit_does_not_release_the_outer_lock(self):
        with state_lock.state_lock():
            handle = state_lock._handle
            with state_lock.state_lock():
                pass
            self.assertIs(state_lock._handle, handle)

    def test_depth_unwinds_on_exception(self):
        with self.assertRaises(RuntimeError):
            with state_lock.state_lock():
                raise RuntimeError("boom")
        self.assertEqual(state_lock._depth, 0)
        self.assertIsNone(state_lock._handle)

    def test_serializes_threads(self):
        order = []

        def worker(name):
            with state_lock.state_lock():
                order.append(f"{name}-in")
                time.sleep(0.05)
                order.append(f"{name}-out")

        threads = [threading.Thread(target=worker, args=(n,)) for n in ("a", "b")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Whoever won, neither critical section is interleaved with the other.
        self.assertEqual(len(order), 4)
        self.assertEqual(order[0][-2:], "in")
        self.assertEqual(order[1], order[0].replace("-in", "-out"))

    def test_degrades_to_a_plain_mutex_without_fcntl(self):
        # Windows has no flock. Losing an append beats refusing to queue at all.
        with patch.object(state_lock, "fcntl", None):
            with state_lock.state_lock():
                self.assertIsNone(state_lock._handle)
        self.assertEqual(state_lock._depth, 0)

    def test_unopenable_lock_file_warns_and_proceeds(self):
        blocked = Path(self.tmpdir.name) / "a-directory"
        blocked.mkdir()
        state_lock.STATE_LOCK_FILE = blocked

        stderr = io.StringIO()
        with redirect_stderr(stderr):
            with state_lock.state_lock():
                self.assertIsNone(state_lock._handle)
        self.assertIn("state lock", stderr.getvalue())


class StateLockAcrossProcessesTests(unittest.TestCase):
    """The property that actually matters: a second *process* is excluded."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.lock_path = Path(self.tmpdir.name) / "state.lock"
        self.original = state_lock.STATE_LOCK_FILE
        state_lock.STATE_LOCK_FILE = self.lock_path
        self.ctx = multiprocessing.get_context("spawn")

    def tearDown(self):
        state_lock.STATE_LOCK_FILE = self.original
        self.tmpdir.cleanup()

    def test_a_second_process_waits(self):
        ready = self.ctx.Event()
        release = self.ctx.Event()
        child = self.ctx.Process(
            target=_hold_lock_in_child, args=(str(self.lock_path), ready, release)
        )
        child.start()
        try:
            self.assertTrue(ready.wait(10), "child never took the lock")

            acquired = threading.Event()

            def try_acquire():
                with state_lock.state_lock():
                    acquired.set()

            waiter = threading.Thread(target=try_acquire)
            waiter.start()
            try:
                self.assertFalse(
                    acquired.wait(0.5), "acquired a lock the child was holding"
                )
                release.set()
                self.assertTrue(acquired.wait(10), "never acquired after the child let go")
            finally:
                release.set()
                waiter.join(10)
        finally:
            release.set()
            child.join(10)
            if child.is_alive():  # pragma: no cover — only on a wedged run
                child.terminate()


if __name__ == "__main__":
    unittest.main()
