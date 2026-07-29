"""Tests for the host-side RPC dispatcher and handlers."""
import tempfile
import unittest
from pathlib import Path

from awsqueueengine.host import rpc
from awsqueueengine.shared import queue as queue_mod
from awsqueueengine.shared import running_state as running_state_mod
from awsqueueengine.shared import deferred_state as deferred_state_mod
from awsqueueengine.shared import completion_state as completion_state_mod
from awsqueueengine.shared import failure_state as failure_state_mod


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
    """Common: redirect the queue-host state files to a temp dir."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        tmp = Path(self.tmpdir.name)
        self._patches = [
            (queue_mod, "QUEUE_FILE", tmp / "queue.json"),
            (running_state_mod, "RUNNING_FILE", tmp / "running.json"),
            (deferred_state_mod, "DEFERRED_FILE", tmp / "deferred.json"),
            (completion_state_mod, "COMPLETED_FILE", tmp / "completed.json"),
            (failure_state_mod, "FAILED_FILE", tmp / "failed.json"),
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


class FailedListTests(_StateFixture):
    def _seed(self, count=3):
        failure_state_mod.save_failed_jobs([
            {
                "job_id": f"F{i}", "host": "eci3", "cmd": f"job {i}", "queue": "default",
                "status": "failed", "exit_code": 1, "failure_reason": "nonzero_exit",
                "failure_detail": "boom", "log_tail": f"line-{i}\ntail-{i}", "failed_at": 100.0 + i,
                "finished_at": 100.0 + i,
            }
            for i in range(count)
        ])

    def test_failed_list_returns_newest_first_without_log_tail(self):
        self._seed()
        resp = rpc.dispatch({"version": 1, "method": "failed_list", "params": {}})
        self.assertTrue(resp["ok"])
        jobs = resp["result"]["jobs"]
        self.assertEqual([j["job_id"] for j in jobs], ["F2", "F1", "F0"])
        self.assertNotIn("log_tail", jobs[0])
        self.assertEqual(jobs[0]["failure_reason"], "nonzero_exit")

    def test_failed_list_honours_limit_and_log_flag(self):
        self._seed()
        resp = rpc.dispatch({"version": 1, "method": "failed_list", "params": {"limit": 2, "log": True}})
        jobs = resp["result"]["jobs"]
        self.assertEqual([j["job_id"] for j in jobs], ["F2", "F1"])
        self.assertEqual(jobs[0]["log_tail"], "line-2\ntail-2")

    def test_failed_list_filters_by_job_id(self):
        self._seed()
        resp = rpc.dispatch({"version": 1, "method": "failed_list", "params": {"job_id": "F1"}})
        self.assertEqual([j["job_id"] for j in resp["result"]["jobs"]], ["F1"])

    def test_failed_list_empty_when_nothing_failed(self):
        resp = rpc.dispatch({"version": 1, "method": "failed_list", "params": {}})
        self.assertEqual(resp["result"], {"jobs": []})

    def test_job_info_reports_failed_state(self):
        self._seed(1)
        resp = rpc.dispatch({"version": 1, "method": "job_info", "params": {"job_id": "F0"}})
        state = resp["result"]["state"]
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state["failure_reason"], "nonzero_exit")
        self.assertEqual(state["exit_code"], "1")

    def test_job_info_prefers_the_newer_terminal_record(self):
        failure_state_mod.save_failed_jobs([
            {"job_id": "R1", "host": "eci1", "cmd": "retry me", "failure_reason": "segfault", "finished_at": 100.0},
        ])
        completion_state_mod.save_completed_jobs([
            {"job_id": "R1", "host": "eci2", "cmd": "retry me", "dur": "00:10:00", "finished_at": 200.0},
        ])
        resp = rpc.dispatch({"version": 1, "method": "job_info", "params": {"job_id": "R1"}})
        state = resp["result"]["state"]
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["host"], "eci2")
        self.assertNotIn("failure_reason", state)


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


class TailTests(unittest.TestCase):
    def setUp(self):
        self._original = rpc.tail_remote_log
        self.calls = []

        def fake_tail(host, lines=200):
            self.calls.append({"host": host, "lines": lines})
            return {"host": host, "ok": True, "tag": "tag-abc", "out": "line1\nline2\n", "err": ""}

        rpc.tail_remote_log = fake_tail

    def tearDown(self):
        rpc.tail_remote_log = self._original

    def test_tail_returns_helper_output_passthrough(self):
        resp = rpc.dispatch({"version": 1, "method": "tail", "params": {"host": "eci5"}})
        self.assertTrue(resp["ok"], resp)
        self.assertEqual(resp["result"]["host"], "eci5")
        self.assertEqual(resp["result"]["tag"], "tag-abc")
        self.assertEqual(resp["result"]["out"], "line1\nline2\n")
        self.assertEqual(self.calls, [{"host": "eci5", "lines": 200}])

    def test_tail_passes_lines_param(self):
        resp = rpc.dispatch({"version": 1, "method": "tail", "params": {"host": "eci5", "lines": 50}})
        self.assertTrue(resp["ok"])
        self.assertEqual(self.calls[0]["lines"], 50)

    def test_tail_clamps_lines_to_max(self):
        resp = rpc.dispatch({"version": 1, "method": "tail", "params": {"host": "eci5", "lines": 999999}})
        self.assertTrue(resp["ok"])
        self.assertEqual(self.calls[0]["lines"], 5000)

    def test_tail_clamps_lines_to_min(self):
        resp = rpc.dispatch({"version": 1, "method": "tail", "params": {"host": "eci5", "lines": 0}})
        self.assertTrue(resp["ok"])
        self.assertEqual(self.calls[0]["lines"], 1)

    def test_tail_requires_host(self):
        resp = rpc.dispatch({"version": 1, "method": "tail", "params": {}})
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "invalid_params")

    def test_tail_rejects_non_int_lines(self):
        resp = rpc.dispatch({"version": 1, "method": "tail", "params": {"host": "eci5", "lines": "many"}})
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "invalid_params")

    def test_tail_returns_unreachable_passthrough(self):
        def unreachable(host, lines=200):
            return {"host": host, "ok": False, "reason": "unreachable"}

        rpc.tail_remote_log = unreachable
        resp = rpc.dispatch({"version": 1, "method": "tail", "params": {"host": "eci5"}})
        # Application-level "host unreachable" still rides on a successful envelope (ok:True at the RPC layer);
        # the caller inspects result.ok to decide what to render.
        self.assertTrue(resp["ok"])
        self.assertFalse(resp["result"]["ok"])
        self.assertEqual(resp["result"]["reason"], "unreachable")


class StatsTests(_StateFixture):
    def setUp(self):
        super().setUp()
        import os
        os.environ["AWSQUEUEENGINE_QUEUES"] = "default=eci1,eci2,eci3;fast=eci10,eci11"
        self.addCleanup(lambda: os.environ.pop("AWSQUEUEENGINE_QUEUES", None))
        # Cooldowns live in MONITOR_STATE_FILE; redirect it under the existing tmpdir.
        from awsqueueengine.host import monitor as monitor_mod
        self._monitor_state_original = monitor_mod.MONITOR_STATE_FILE
        monitor_mod.MONITOR_STATE_FILE = Path(self.tmpdir.name) / "monitor_state.json"
        self.addCleanup(lambda: setattr(monitor_mod, "MONITOR_STATE_FILE", self._monitor_state_original))

    def _call(self):
        resp = rpc.dispatch({"version": 1, "method": "stats", "params": {}})
        self.assertTrue(resp["ok"], resp)
        return resp["result"]

    def test_stats_empty_state_reports_zero_running_full_pool_empty(self):
        result = self._call()
        self.assertEqual(result["running_count"], 0)
        self.assertEqual(result["queued_count"], 0)
        self.assertEqual(result["host_total"], 5)
        self.assertEqual(result["host_pool"], ["eci1", "eci10", "eci11", "eci2", "eci3"])
        self.assertEqual(result["running_hosts"], [])
        self.assertEqual(result["cooldown_hosts"], [])
        self.assertEqual(result["fraction_empty"], 1.0)
        # Configured queues are surfaced even with 0 jobs each.
        self.assertEqual(result["queued_by_queue"], {"default": 0, "fast": 0})
        self.assertEqual(set(result["queue_host_map"].keys()), {"default", "fast"})

    def test_stats_counts_running_and_queued(self):
        running_state_mod.save_running_jobs({
            "eci1": {"cmd": "j1", "priority": 0},
            "eci10": {"cmd": "j2", "priority": 50},
        })
        queue_mod.save_queue([
            {"cmd": "a", "queue": "default", "job_id": "Q1"},
            {"cmd": "b", "queue": "default", "job_id": "Q2"},
            {"cmd": "c", "queue": "fast", "job_id": "Q3"},
        ])
        result = self._call()
        self.assertEqual(result["running_count"], 2)
        self.assertEqual(result["queued_count"], 3)
        self.assertEqual(result["host_total"], 5)
        self.assertEqual(set(result["running_hosts"]), {"eci1", "eci10"})
        self.assertAlmostEqual(result["fraction_empty"], 3 / 5)
        self.assertEqual(result["queued_by_queue"], {"default": 2, "fast": 1})

    def test_stats_surfaces_cooldown_hosts(self):
        # Write a future cooldown for eci3 directly into the redirected state file.
        import json, time
        from awsqueueengine.host import monitor as monitor_mod
        monitor_mod.MONITOR_STATE_FILE.write_text(json.dumps({
            "host_disabled_until": {"eci3": time.time() + 3600},
        }))
        result = self._call()
        self.assertEqual(result["cooldown_hosts"], ["eci3"])

    def test_stats_with_empty_pool_avoids_divide_by_zero(self):
        original = rpc._load_queue_host_map
        rpc._load_queue_host_map = lambda: {}
        try:
            result = self._call()
        finally:
            rpc._load_queue_host_map = original
        self.assertEqual(result["host_total"], 0)
        self.assertEqual(result["fraction_empty"], 0.0)


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

    def test_enqueue_persists_mps_flag(self):
        resp = rpc.dispatch({
            "version": 1, "method": "enqueue",
            "params": {"cmd": "echo hi", "queue": "default", "mps": True},
        })
        self.assertTrue(resp["ok"], resp)
        self.assertTrue(queue_mod.load_queue()[0]["mps"])

    def test_enqueue_defaults_mps_to_false(self):
        resp = rpc.dispatch({
            "version": 1, "method": "enqueue",
            "params": {"cmd": "echo hi", "queue": "default"},
        })
        self.assertTrue(resp["ok"], resp)
        self.assertFalse(queue_mod.load_queue()[0]["mps"])

    def test_enqueue_rejects_non_bool_mps(self):
        resp = rpc.dispatch({
            "version": 1, "method": "enqueue",
            "params": {"cmd": "echo hi", "queue": "default", "mps": "yes"},
        })
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "invalid_params")

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
