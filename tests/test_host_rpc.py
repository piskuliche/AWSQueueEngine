"""Tests for the host-side RPC dispatcher and handlers."""
import tempfile
import unittest
from pathlib import Path

from awsqueueengine.host import rpc
from awsqueueengine.shared import queue as queue_mod
from awsqueueengine.shared import running_state as running_state_mod
from awsqueueengine.shared import deferred_state as deferred_state_mod


class DispatchEnvelopeTests(unittest.TestCase):
    def test_non_dict_request_returns_bad_request(self):
        resp = rpc.dispatch("not a dict")
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "bad_request")

    def test_wrong_version_returns_bad_request(self):
        resp = rpc.dispatch({"version": 99, "method": "list", "params": {}})
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "bad_request")

    def test_missing_method_returns_bad_request(self):
        resp = rpc.dispatch({"version": 1, "params": {}})
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "bad_request")

    def test_non_object_params_returns_bad_request(self):
        resp = rpc.dispatch({"version": 1, "method": "list", "params": ["nope"]})
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "bad_request")

    def test_unknown_method_returns_unknown_method(self):
        resp = rpc.dispatch({"version": 1, "method": "no_such_thing", "params": {}})
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "unknown_method")

    def test_handler_exception_serializes_as_internal_error(self):
        original = rpc.METHODS.get("list")
        rpc.METHODS["list"] = lambda params: (_ for _ in ()).throw(RuntimeError("boom"))
        try:
            resp = rpc.dispatch({"version": 1, "method": "list", "params": {}})
        finally:
            rpc.METHODS["list"] = original
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "internal")
        self.assertIn("RuntimeError", resp["error"]["message"])
        self.assertIn("boom", resp["error"]["message"])


class _StateFixture(unittest.TestCase):
    """Common: redirect QUEUE_FILE / RUNNING_FILE / DEFERRED_FILE to a temp dir."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        tmp = Path(self.tmpdir.name)
        self._patches = [
            (queue_mod, "QUEUE_FILE", tmp / "queue.json"),
            (running_state_mod, "RUNNING_FILE", tmp / "running.json"),
            (deferred_state_mod, "DEFERRED_FILE", tmp / "deferred.json"),
        ]
        self._originals = []
        for module, name, replacement in self._patches:
            self._originals.append((module, name, getattr(module, name)))
            setattr(module, name, replacement)

    def tearDown(self):
        for module, name, original in self._originals:
            setattr(module, name, original)
        self.tmpdir.cleanup()


class ListAndQstatTests(_StateFixture):
    def test_list_returns_empty_jobs_on_empty_queue(self):
        resp = rpc.dispatch({"version": 1, "method": "list", "params": {}})
        self.assertTrue(resp["ok"])
        self.assertEqual(resp["result"], {"jobs": []})

    def test_list_returns_normalized_jobs(self):
        queue_mod.save_queue([
            {"cmd": "echo hi", "priority": 5, "queue": "default", "job_id": "JOB1"},
        ])
        resp = rpc.dispatch({"version": 1, "method": "list", "params": {}})
        self.assertTrue(resp["ok"])
        jobs = resp["result"]["jobs"]
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["job_id"], "JOB1")
        self.assertEqual(jobs[0]["priority"], 5)
        self.assertEqual(jobs[0]["queue"], "default")

    def test_qstat_returns_running_jobs_with_started_at(self):
        running_state_mod.save_running_jobs({
            "eci5": {"cmd": "run.sh", "priority": 0, "started_at": 1715537422.5},
        })
        resp = rpc.dispatch({"version": 1, "method": "qstat", "params": {}})
        self.assertTrue(resp["ok"])
        running = resp["result"]["running"]
        self.assertIn("eci5", running)
        self.assertEqual(running["eci5"]["started_at"], 1715537422.5)


class QdelTests(_StateFixture):
    def test_qdel_removes_indexed_entries(self):
        queue_mod.save_queue([
            {"cmd": "first", "job_id": "A"},
            {"cmd": "second", "job_id": "B"},
            {"cmd": "third", "job_id": "C"},
        ])
        resp = rpc.dispatch({"version": 1, "method": "qdel", "params": {"indices": [1, 3]}})
        self.assertTrue(resp["ok"])
        removed = resp["result"]["removed"]
        self.assertEqual([r["index"] for r in removed], [1, 3])
        remaining = queue_mod.load_queue()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["cmd"], "second")

    def test_qdel_empty_queue_returns_not_found(self):
        resp = rpc.dispatch({"version": 1, "method": "qdel", "params": {"indices": [1]}})
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "not_found")

    def test_qdel_out_of_range_returns_conflict(self):
        queue_mod.save_queue([{"cmd": "only", "job_id": "A"}])
        resp = rpc.dispatch({"version": 1, "method": "qdel", "params": {"indices": [5]}})
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "conflict")

    def test_qdel_requires_non_empty_indices_list(self):
        resp = rpc.dispatch({"version": 1, "method": "qdel", "params": {}})
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "invalid_params")


class JobInfoTests(_StateFixture):
    def test_job_info_returns_null_state_for_unknown_id(self):
        resp = rpc.dispatch({"version": 1, "method": "job_info", "params": {"job_id": "nope"}})
        self.assertTrue(resp["ok"])
        self.assertIsNone(resp["result"]["state"])

    def test_job_info_finds_queued_job(self):
        queue_mod.save_queue([{"cmd": "echo", "job_id": "QUEUED1"}])
        resp = rpc.dispatch({"version": 1, "method": "job_info", "params": {"job_id": "QUEUED1"}})
        self.assertTrue(resp["ok"])
        self.assertEqual(resp["result"]["state"]["status"], "queued")
        self.assertEqual(resp["result"]["state"]["queue_position"], 1)

    def test_job_info_requires_string_job_id(self):
        resp = rpc.dispatch({"version": 1, "method": "job_info", "params": {}})
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "invalid_params")


class DeferredListTests(_StateFixture):
    def test_deferred_list_carries_deferred_at_last_host_last_error(self):
        deferred_state_mod.save_deferred_jobs([
            {
                "cmd": "boom", "job_id": "D1", "priority": 0, "queue": "default",
                "deferred_at": 1234.5, "last_host": "eci7", "last_error": "host_storage: no scratch",
            },
        ])
        resp = rpc.dispatch({"version": 1, "method": "deferred_list", "params": {}})
        self.assertTrue(resp["ok"])
        jobs = resp["result"]["jobs"]
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["deferred_at"], 1234.5)
        self.assertEqual(jobs[0]["last_host"], "eci7")
        self.assertEqual(jobs[0]["last_error"], "host_storage: no scratch")


class RequeueDeferredTests(_StateFixture):
    def test_requeue_all_moves_back_to_queue_and_resets_failures(self):
        deferred_state_mod.save_deferred_jobs([
            {"cmd": "j1", "job_id": "D1", "submit_failures": 3, "queue": "default"},
            {"cmd": "j2", "job_id": "D2", "submit_failures": 9, "queue": "default"},
        ])
        resp = rpc.dispatch({"version": 1, "method": "requeue_deferred", "params": {"all": True}})
        self.assertTrue(resp["ok"])
        moved = resp["result"]["moved"]
        self.assertEqual(len(moved), 2)
        for entry in moved:
            self.assertEqual(entry["item"]["submit_failures"], 0)
        # All moved back into the main queue.
        self.assertEqual([j["job_id"] for j in queue_mod.load_queue()], ["D1", "D2"])
        # Deferred queue now empty.
        self.assertEqual(deferred_state_mod.load_deferred_jobs(), [])

    def test_requeue_with_drop_does_not_enqueue(self):
        deferred_state_mod.save_deferred_jobs([{"cmd": "dropme", "job_id": "D1", "queue": "default"}])
        resp = rpc.dispatch({"version": 1, "method": "requeue_deferred", "params": {"all": True, "drop": True}})
        self.assertTrue(resp["ok"])
        self.assertEqual(resp["result"]["action"], "dropped")
        self.assertEqual(queue_mod.load_queue(), [])

    def test_requeue_all_and_indices_conflict_rejected(self):
        resp = rpc.dispatch({"version": 1, "method": "requeue_deferred", "params": {"all": True, "indices": [1]}})
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "invalid_params")

    def test_requeue_neither_all_nor_indices_rejected(self):
        resp = rpc.dispatch({"version": 1, "method": "requeue_deferred", "params": {}})
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "invalid_params")


class EnqueueTests(_StateFixture):
    def setUp(self):
        super().setUp()
        # Provide a queue config so handle_enqueue's validation passes.
        import os
        os.environ["AWSQUEUEENGINE_QUEUES"] = "default=eci1,eci2,eci3;fast=eci10,eci11"
        self.addCleanup(lambda: os.environ.pop("AWSQUEUEENGINE_QUEUES", None))

    def test_enqueue_succeeds_and_returns_job_id(self):
        resp = rpc.dispatch({
            "version": 1, "method": "enqueue",
            "params": {"cmd": "echo hi", "queue": "default", "job_id": "CUSTOM-ID"},
        })
        self.assertTrue(resp["ok"], resp)
        self.assertEqual(resp["result"]["job_id"], "CUSTOM-ID")
        self.assertEqual(resp["result"]["queue"], "default")
        # Job landed in the queue.
        queue_items = queue_mod.load_queue()
        self.assertEqual(len(queue_items), 1)
        self.assertEqual(queue_items[0]["cmd"], "echo hi")
        self.assertEqual(queue_items[0]["job_id"], "CUSTOM-ID")

    def test_enqueue_rejects_unknown_queue(self):
        resp = rpc.dispatch({
            "version": 1, "method": "enqueue",
            "params": {"cmd": "echo hi", "queue": "nope"},
        })
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "invalid_params")
        self.assertIn("unknown queue", resp["error"]["message"])

    def test_enqueue_rejects_invalid_hosts(self):
        resp = rpc.dispatch({
            "version": 1, "method": "enqueue",
            "params": {"cmd": "echo hi", "queue": "default", "hosts": ["eci99"]},
        })
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "invalid_params")
        self.assertIn("invalid host(s)", resp["error"]["message"])

    def test_enqueue_with_high_priority_maps_to_100(self):
        resp = rpc.dispatch({
            "version": 1, "method": "enqueue",
            "params": {"cmd": "echo hi", "queue": "default", "high_priority": True},
        })
        self.assertTrue(resp["ok"])
        self.assertEqual(queue_mod.load_queue()[0]["priority"], 100)

    def test_enqueue_with_payload_s3_uri_drops_local_payload_path(self):
        resp = rpc.dispatch({
            "version": 1, "method": "enqueue",
            "params": {
                "cmd": "echo", "queue": "default",
                "payload": "/local/path/that/host/cannot/see",
                "payload_s3_uri": "s3://bucket/key.tar.gz",
            },
        })
        self.assertTrue(resp["ok"])
        item = queue_mod.load_queue()[0]
        self.assertIsNone(item["payload"])
        self.assertEqual(item["payload_s3_uri"], "s3://bucket/key.tar.gz")

    def test_enqueue_requires_cmd(self):
        resp = rpc.dispatch({"version": 1, "method": "enqueue", "params": {"queue": "default"}})
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "invalid_params")


if __name__ == "__main__":
    unittest.main()
