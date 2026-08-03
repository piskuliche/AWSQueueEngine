"""Every queue mutation must re-read the queue *inside* the state lock.

Taking the lock around a copy loaded beforehand fixes nothing — the stale copy
still gets written back, which is exactly the lost update in #21. These tests
assert the ordering property directly (was the lock held at the moment we read
and wrote?) rather than trying to stage a race, which in one process cannot be
made to fail deterministically anyway. The real cross-process proof lives in
`test_host_state_concurrency.py`.
"""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from awsqueueengine.shared import deferred_state, queue, state_lock


class _LockWitness:
    """Records whether the state lock was held on each load and each save."""

    def __init__(self, module, load_name, save_name):
        self.module = module
        self.load_name = load_name
        self.save_name = save_name
        self.events = []

    def __enter__(self):
        real_load = getattr(self.module, self.load_name)
        real_save = getattr(self.module, self.save_name)

        def load(*args, **kwargs):
            self.events.append(("load", state_lock._depth > 0))
            return real_load(*args, **kwargs)

        def save(*args, **kwargs):
            self.events.append(("save", state_lock._depth > 0))
            return real_save(*args, **kwargs)

        self._patches = [
            patch.object(self.module, self.load_name, load),
            patch.object(self.module, self.save_name, save),
        ]
        for p in self._patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in self._patches:
            p.stop()
        return False

    def assert_all_locked(self, testcase):
        testcase.assertTrue(self.events, "nothing read or wrote the state file")
        unlocked = [name for name, locked in self.events if not locked]
        testcase.assertEqual(unlocked, [], f"ran outside the state lock: {unlocked}")

    def assert_read_before_write(self, testcase):
        names = [name for name, _ in self.events]
        testcase.assertIn("load", names)
        testcase.assertIn("save", names)
        testcase.assertLess(names.index("load"), names.index("save"))


class QueueMutationLockingTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self.original_queue_file = queue.QUEUE_FILE
        self.original_deferred_file = deferred_state.DEFERRED_FILE
        queue.QUEUE_FILE = self.root / "queue.json"
        deferred_state.DEFERRED_FILE = self.root / "deferred.json"

    def tearDown(self):
        queue.QUEUE_FILE = self.original_queue_file
        deferred_state.DEFERRED_FILE = self.original_deferred_file
        self.tmpdir.cleanup()

    def _witness(self):
        return _LockWitness(queue, "load_queue", "save_queue")

    def test_enqueue_item_reads_and_writes_under_the_lock(self):
        with self._witness() as witness:
            queue.enqueue_item({"cmd": "a", "job_id": "j1"})
        witness.assert_all_locked(self)
        witness.assert_read_before_write(self)

    def test_requeue_front_and_back_are_locked(self):
        queue.enqueue_item({"cmd": "middle", "job_id": "j0"})
        with self._witness() as witness:
            queue.requeue_front({"cmd": "first", "job_id": "j1"})
            queue.requeue_back({"cmd": "last", "job_id": "j2"})
        witness.assert_all_locked(self)
        self.assertEqual(
            [item["job_id"] for item in queue.load_queue()], ["j1", "j0", "j2"]
        )

    def test_dequeue_selects_and_pops_under_one_lock(self):
        queue.enqueue_item({"cmd": "low", "job_id": "j1", "priority": 0})
        queue.enqueue_item({"cmd": "high", "job_id": "j2", "priority": 100})
        with self._witness() as witness:
            item = queue.dequeue()
        self.assertEqual(item["job_id"], "j2")
        witness.assert_all_locked(self)
        witness.assert_read_before_write(self)

    def test_dequeue_for_host_is_locked(self):
        queue.enqueue_item({"cmd": "a", "job_id": "j1", "queue": "gpu"})
        with self._witness() as witness:
            item = queue.dequeue_for_host("eci1", queue_host_map={"gpu": ["eci1"]})
        self.assertEqual(item["job_id"], "j1")
        witness.assert_all_locked(self)

    def test_empty_queue_dequeue_takes_the_lock_and_releases_it(self):
        self.assertIsNone(queue.dequeue())
        self.assertIsNone(queue.dequeue_for_host("eci1"))
        self.assertEqual(state_lock._depth, 0)

    def test_delete_queue_selection_resolves_against_a_fresh_read(self):
        queue.enqueue_item({"cmd": "a", "job_id": "job-a"})
        queue.enqueue_item({"cmd": "b", "job_id": "job-b"})

        with self._witness() as witness:
            removed = queue.delete_queue_selection(job_ids=["job-b"])

        self.assertEqual([item["job_id"] for _idx, item, _token in removed], ["job-b"])
        witness.assert_all_locked(self)
        witness.assert_read_before_write(self)
        self.assertEqual([item["job_id"] for item in queue.load_queue()], ["job-a"])

    def test_delete_queue_selection_leaves_the_queue_alone_on_a_bad_selector(self):
        queue.enqueue_item({"cmd": "a", "job_id": "job-a"})
        with self.assertRaises(queue.QueueSelectionError):
            queue.delete_queue_selection(job_ids=["nope"])
        self.assertEqual(len(queue.load_queue()), 1)
        self.assertEqual(state_lock._depth, 0)

    def test_deferred_pop_then_enqueue_nests_without_deadlocking(self):
        deferred_state.append_deferred_job({"cmd": "a", "job_id": "job-a"})
        with state_lock.state_lock():
            popped = deferred_state.pop_all_deferred()
            for _idx, record in popped:
                queue.enqueue_item(record)
        self.assertEqual([item["job_id"] for item in queue.load_queue()], ["job-a"])
        self.assertEqual(deferred_state.load_deferred_jobs(), [])
        self.assertEqual(state_lock._depth, 0)


class ClaimQueuedJobTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.original_queue_file = queue.QUEUE_FILE
        queue.QUEUE_FILE = Path(self.tmpdir.name) / "queue.json"

    def tearDown(self):
        queue.QUEUE_FILE = self.original_queue_file
        self.tmpdir.cleanup()

    def test_claims_by_job_id_regardless_of_position(self):
        queue.enqueue_item({"cmd": "a", "job_id": "job-a"})
        queue.enqueue_item({"cmd": "b", "job_id": "job-b"})
        # Something else jumps the queue after the snapshot was taken.
        queue.requeue_front({"cmd": "urgent", "job_id": "job-c"})

        claimed = queue.claim_queued_job("job-b")

        self.assertEqual(claimed["cmd"], "b")
        self.assertEqual([item["job_id"] for item in queue.load_queue()], ["job-c", "job-a"])

    def test_returns_none_when_the_job_is_already_gone(self):
        queue.enqueue_item({"cmd": "a", "job_id": "job-a"})
        self.assertIsNone(queue.claim_queued_job("job-b"))
        self.assertEqual(len(queue.load_queue()), 1)

    def test_falls_back_to_item_identity_for_a_job_without_an_id(self):
        legacy = {"cmd": "legacy", "priority": 7}
        queue.enqueue_item(legacy)

        claimed = queue.claim_queued_job(None, fallback_item=legacy)

        self.assertEqual(claimed["cmd"], "legacy")
        self.assertEqual(queue.load_queue(), [])

    def test_no_id_and_no_fallback_claims_nothing(self):
        queue.enqueue_item({"cmd": "a"})
        self.assertIsNone(queue.claim_queued_job(None))
        self.assertEqual(len(queue.load_queue()), 1)


if __name__ == "__main__":
    unittest.main()
