"""The only test that actually proves the state lock works.

Everything else in the suite asserts *structure* — that the load happens inside
the lock. Structure is not the property #21 is about; the property is that N
real processes hammering `enqueue` while another dispatches cannot lose a job.
That needs real processes, because `flock` is a kernel object and a single
Python process can only ever pretend to contend for it.

Marked slow and excluded from the default run (see pytest.ini). Run it with::

    pytest -m slow
"""
import multiprocessing
import tempfile
import time
import unittest
from pathlib import Path

import pytest

#: Jobs each submitter process enqueues. Sized so the run stays a couple of
#: seconds while still overlapping the dispatcher many times over.
SUBMITTERS = 6
JOBS_PER_SUBMITTER = 40
TOTAL_JOBS = SUBMITTERS * JOBS_PER_SUBMITTER


def _configure(queue_path, lock_path):
    """Point a freshly-spawned child at the test's state files."""
    from awsqueueengine.shared import queue as queue_mod
    from awsqueueengine.shared import state_lock as state_lock_mod

    queue_mod.QUEUE_FILE = Path(queue_path)
    state_lock_mod.STATE_LOCK_FILE = Path(lock_path)
    return queue_mod


def _submit_worker(queue_path, lock_path, worker_index, start):
    queue_mod = _configure(queue_path, lock_path)
    start.wait(30)
    for job_index in range(JOBS_PER_SUBMITTER):
        queue_mod.enqueue_item(
            {"cmd": f"job-{worker_index}-{job_index}", "job_id": f"w{worker_index}-j{job_index}"}
        )


def _dispatch_worker(queue_path, lock_path, start, stop, dispatched):
    """Stand in for the monitor: dequeue as fast as it can until told to stop."""
    queue_mod = _configure(queue_path, lock_path)
    start.wait(30)
    taken = []
    while not stop.is_set():
        item = queue_mod.dequeue()
        if item is None:
            time.sleep(0.001)
            continue
        taken.append(item["job_id"])
    # Drain whatever is left so the accounting below is exact.
    while True:
        item = queue_mod.dequeue()
        if item is None:
            break
        taken.append(item["job_id"])
    dispatched.extend(taken)


@pytest.mark.slow
class ConcurrentEnqueueTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.queue_path = Path(self.tmpdir.name) / "queue.json"
        self.lock_path = Path(self.tmpdir.name) / "state.lock"
        # spawn, not fork: a forked child would inherit this process's patched
        # module state and prove less than it looks like it does.
        self.ctx = multiprocessing.get_context("spawn")

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_no_job_is_lost_when_submits_race_a_dispatcher(self):
        manager = self.ctx.Manager()
        dispatched = manager.list()
        start = self.ctx.Event()
        stop = self.ctx.Event()

        dispatcher = self.ctx.Process(
            target=_dispatch_worker,
            args=(str(self.queue_path), str(self.lock_path), start, stop, dispatched),
        )
        submitters = [
            self.ctx.Process(
                target=_submit_worker,
                args=(str(self.queue_path), str(self.lock_path), index, start),
            )
            for index in range(SUBMITTERS)
        ]

        dispatcher.start()
        for process in submitters:
            process.start()
        start.set()

        for process in submitters:
            process.join(120)
            self.assertEqual(process.exitcode, 0, "a submitter died")
        stop.set()
        dispatcher.join(120)
        self.assertEqual(dispatcher.exitcode, 0, "the dispatcher died")

        from awsqueueengine.shared import queue as queue_mod

        original = queue_mod.QUEUE_FILE
        queue_mod.QUEUE_FILE = self.queue_path
        try:
            still_queued = [item["job_id"] for item in queue_mod.load_queue()]
        finally:
            queue_mod.QUEUE_FILE = original

        seen = list(dispatched) + still_queued
        expected = {
            f"w{worker}-j{job}"
            for worker in range(SUBMITTERS)
            for job in range(JOBS_PER_SUBMITTER)
        }

        self.assertEqual(len(seen), len(set(seen)), "a job was dispatched twice")
        missing = sorted(expected - set(seen))
        self.assertEqual(missing, [], f"{len(missing)} of {TOTAL_JOBS} jobs were lost")

        # A surviving `.tmp.<pid>` would mean an atomic write was interrupted.
        litter = sorted(p.name for p in Path(self.tmpdir.name).iterdir() if ".tmp." in p.name)
        self.assertEqual(litter, [])
