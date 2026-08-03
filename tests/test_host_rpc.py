"""Tests for the host-side RPC dispatcher and handlers."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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

    def test_qdel_requires_a_selector(self):
        resp = rpc.dispatch({"version": 1, "method": "qdel", "params": {}})
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "invalid_params")

    def _batched_queue(self):
        queue_mod.save_queue([
            {"cmd": "a", "job_id": "A", "array_id": "ffpopt-IDC"},
            {"cmd": "loose", "job_id": "B"},
            {"cmd": "c", "job_id": "C", "array_id": "ffpopt-IDC"},
        ])

    def test_qdel_by_array_removes_every_member_and_nothing_else(self):
        self._batched_queue()
        resp = rpc.dispatch({
            "version": 1, "method": "qdel", "params": {"array_id": "ffpopt-IDC"},
        })
        self.assertTrue(resp["ok"], resp)
        self.assertEqual([r["index"] for r in resp["result"]["removed"]], [1, 3])
        remaining = queue_mod.load_queue()
        self.assertEqual([item["job_id"] for item in remaining], ["B"])

    def test_qdel_by_unknown_array_is_not_found_and_removes_nothing(self):
        self._batched_queue()
        resp = rpc.dispatch({
            "version": 1, "method": "qdel", "params": {"array_id": "nope"},
        })
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "not_found")
        self.assertEqual(len(queue_mod.load_queue()), 3)

    def test_qdel_by_array_reports_members_it_could_not_reach(self):
        """qdel only ever touched the queue. "Removed 1 job(s)" would otherwise
        read as "the batch is cancelled"."""
        self._batched_queue()
        running_state_mod.save_running_jobs({
            "eci5": {"cmd": "a", "job_id": "R1", "array_id": "ffpopt-IDC"},
            "eci6": {"cmd": "other", "job_id": "R2"},
        })
        resp = rpc.dispatch({
            "version": 1, "method": "qdel", "params": {"array_id": "ffpopt-IDC"},
        })
        self.assertTrue(resp["ok"], resp)
        self.assertEqual(
            resp["result"]["running"], [{"host": "eci5", "job_id": "R1"}],
        )

    def test_qdel_by_other_selectors_reports_no_running_key(self):
        # Only the batch selector invites the "did this cancel everything?"
        # question, so only it pays for the extra state read.
        self._batched_queue()
        resp = rpc.dispatch({"version": 1, "method": "qdel", "params": {"job_ids": ["A"]}})
        self.assertTrue(resp["ok"])
        self.assertNotIn("running", resp["result"])

    def test_qdel_rejects_array_combined_with_job_ids(self):
        self._batched_queue()
        resp = rpc.dispatch({
            "version": 1, "method": "qdel",
            "params": {"array_id": "ffpopt-IDC", "job_ids": ["A"]},
        })
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "invalid_params")
        self.assertEqual(len(queue_mod.load_queue()), 3)

    def _three_jobs(self):
        queue_mod.save_queue([
            {"cmd": "first", "job_id": "aaa111"},
            {"cmd": "second", "job_id": "bbb222", "queue": "fast"},
            {"cmd": "third", "job_id": "ccc333", "queue": "fast"},
        ])

    def test_qdel_removes_entries_by_job_id(self):
        self._three_jobs()
        resp = rpc.dispatch({
            "version": 1, "method": "qdel", "params": {"job_ids": ["aaa111", "ccc333"]},
        })
        self.assertTrue(resp["ok"])
        removed = resp["result"]["removed"]
        self.assertEqual([r["index"] for r in removed], [1, 3])
        self.assertEqual([r["selector"] for r in removed], ["aaa111", "ccc333"])
        self.assertEqual([i["cmd"] for i in queue_mod.load_queue()], ["second"])

    def test_qdel_accepts_a_unique_job_id_prefix(self):
        self._three_jobs()
        resp = rpc.dispatch({"version": 1, "method": "qdel", "params": {"job_ids": ["bbb"]}})
        self.assertTrue(resp["ok"])
        self.assertEqual(
            [i["cmd"] for i in queue_mod.load_queue()], ["first", "third"]
        )

    def test_qdel_by_queue_name_removes_the_whole_queue(self):
        self._three_jobs()
        resp = rpc.dispatch({"version": 1, "method": "qdel", "params": {"queue": "fast"}})
        self.assertTrue(resp["ok"])
        self.assertEqual([r["index"] for r in resp["result"]["removed"]], [2, 3])
        self.assertEqual([i["cmd"] for i in queue_mod.load_queue()], ["first"])

    def test_qdel_unknown_job_id_leaves_the_queue_untouched(self):
        self._three_jobs()
        resp = rpc.dispatch({
            "version": 1, "method": "qdel", "params": {"job_ids": ["aaa111", "nosuch"]},
        })
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "not_found")
        self.assertEqual(len(queue_mod.load_queue()), 3)

    def test_qdel_ambiguous_prefix_returns_conflict(self):
        queue_mod.save_queue([
            {"cmd": "first", "job_id": "aaa111"},
            {"cmd": "second", "job_id": "aaa222"},
        ])
        resp = rpc.dispatch({"version": 1, "method": "qdel", "params": {"job_ids": ["aaa"]}})
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "conflict")
        self.assertEqual(len(queue_mod.load_queue()), 2)

    def test_qdel_rejects_mixed_selectors(self):
        self._three_jobs()
        resp = rpc.dispatch({
            "version": 1, "method": "qdel", "params": {"job_ids": ["aaa111"], "indices": [2]},
        })
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "invalid_params")
        self.assertEqual(len(queue_mod.load_queue()), 3)

    def test_qdel_explains_a_job_that_is_already_running(self):
        self._three_jobs()
        running_state_mod.save_running_jobs({
            "eci5": {"cmd": "gone", "job_id": "ddd444", "started_at": 1.0},
        })
        resp = rpc.dispatch({"version": 1, "method": "qdel", "params": {"job_ids": ["ddd444"]}})
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "not_found")
        self.assertIn("already running on eci5", resp["error"]["message"])


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


class JobInfoBatchTests(_StateFixture):
    def _batch(self, job_ids):
        return rpc.dispatch({
            "version": 1, "method": "job_info_batch", "params": {"job_ids": job_ids},
        })

    def test_resolves_mixed_states_in_one_call(self):
        queue_mod.save_queue([{"cmd": "echo", "job_id": "Q1"}])
        running_state_mod.save_running_jobs({
            "eci5": {"cmd": "run.sh", "job_id": "R1", "started_at": 1715537422.5},
        })
        completion_state_mod.save_completed_jobs([
            {"job_id": "C1", "host": "eci3", "status": "completed", "finished_at": 1715537600.0},
        ])
        resp = self._batch(["Q1", "R1", "C1", "GONE"])
        self.assertTrue(resp["ok"])
        states = resp["result"]["states"]
        self.assertEqual(states["Q1"]["status"], "queued")
        self.assertEqual(states["R1"]["status"], "running")
        self.assertEqual(states["C1"]["status"], "completed")
        self.assertIsNone(states["GONE"])
        self.assertEqual(resp["result"]["skipped"], [])

    def test_every_requested_id_is_present_so_null_is_unambiguous(self):
        resp = self._batch(["A", "B"])
        self.assertEqual(set(resp["result"]["states"]), {"A", "B"})

    def test_duplicates_and_whitespace_collapse(self):
        queue_mod.save_queue([{"cmd": "echo", "job_id": "Q1"}])
        resp = self._batch(["Q1", " Q1 ", "Q1"])
        self.assertEqual(set(resp["result"]["states"]), {"Q1"})

    def test_empty_job_ids_is_invalid_params(self):
        for params in ({}, {"job_ids": []}, {"job_ids": ["", "  "]}):
            resp = rpc.dispatch({"version": 1, "method": "job_info_batch", "params": params})
            self.assertFalse(resp["ok"], msg=f"expected failure for {params!r}")
            self.assertEqual(resp["error"]["code"], "invalid_params")

    def test_non_list_job_ids_is_invalid_params(self):
        resp = self._batch("Q1")
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "invalid_params")

    def test_over_cap_is_clamped_and_the_remainder_named_in_skipped(self):
        job_ids = [f"J{i}" for i in range(rpc.MAX_JOB_INFO_BATCH + 5)]
        resp = self._batch(job_ids)
        self.assertTrue(resp["ok"])
        self.assertEqual(len(resp["result"]["states"]), rpc.MAX_JOB_INFO_BATCH)
        self.assertEqual(resp["result"]["skipped"], job_ids[rpc.MAX_JOB_INFO_BATCH:])


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

    def test_enqueue_persists_the_array_id(self):
        resp = rpc.dispatch({
            "version": 1, "method": "enqueue",
            "params": {"cmd": "echo hi", "queue": "default", "array_id": "ffpopt-IDC"},
        })
        self.assertTrue(resp["ok"], resp)
        self.assertEqual(resp["result"]["array_id"], "ffpopt-IDC")
        self.assertEqual(queue_mod.load_queue()[0]["array_id"], "ffpopt-IDC")

    def test_enqueue_without_an_array_id_stores_none(self):
        resp = rpc.dispatch({
            "version": 1, "method": "enqueue",
            "params": {"cmd": "echo hi", "queue": "default"},
        })
        self.assertTrue(resp["ok"], resp)
        self.assertIsNone(queue_mod.load_queue()[0]["array_id"])

    def test_enqueue_rejects_a_non_string_array_id(self):
        resp = rpc.dispatch({
            "version": 1, "method": "enqueue",
            "params": {"cmd": "echo hi", "queue": "default", "array_id": 7},
        })
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "invalid_params")

    def test_enqueue_many_enqueues_the_whole_batch_in_order(self):
        resp = rpc.dispatch({
            "version": 1, "method": "enqueue_many",
            "params": {"jobs": [
                {"cmd": f"job {i}", "queue": "default", "job_id": f"J{i}"}
                for i in range(3)
            ]},
        })
        self.assertTrue(resp["ok"], resp)
        self.assertEqual(resp["result"]["enqueued"], 3)
        self.assertEqual([r["job_id"] for r in resp["result"]["results"]],
                         ["J0", "J1", "J2"])
        self.assertEqual([item["job_id"] for item in queue_mod.load_queue()],
                         ["J0", "J1", "J2"])

    def test_enqueue_many_writes_the_queue_once(self):
        """The host has no lock around state mutation, so a batch doing one
        read-modify-write instead of N shrinks that window rather than widening
        it (issue #21)."""
        with patch.object(queue_mod, "save_queue", wraps=queue_mod.save_queue) as save:
            rpc.dispatch({
                "version": 1, "method": "enqueue_many",
                "params": {"jobs": [{"cmd": f"j{i}", "queue": "default"} for i in range(10)]},
            })
        self.assertEqual(save.call_count, 1)

    def test_enqueue_many_is_all_or_nothing_on_a_bad_queue(self):
        """Every job in a --payload-glob batch shares its --queue, so a bad one
        is a whole-batch user error; half-enqueuing 105 jobs is the worst
        available outcome."""
        resp = rpc.dispatch({
            "version": 1, "method": "enqueue_many",
            "params": {"jobs": [
                {"cmd": "ok", "queue": "default"},
                {"cmd": "bad", "queue": "nope"},
                {"cmd": "ok2", "queue": "default"},
            ]},
        })
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["error"]["code"], "invalid_params")
        self.assertIn("job 1", resp["error"]["message"])
        self.assertIn("unknown queue", resp["error"]["message"])
        self.assertEqual(queue_mod.load_queue(), [])

    def test_enqueue_many_is_all_or_nothing_on_a_missing_command(self):
        resp = rpc.dispatch({
            "version": 1, "method": "enqueue_many",
            "params": {"jobs": [{"cmd": "ok", "queue": "default"}, {"queue": "default"}]},
        })
        self.assertFalse(resp["ok"])
        self.assertIn("job 1", resp["error"]["message"])
        self.assertEqual(queue_mod.load_queue(), [])

    def test_enqueue_many_carries_every_per_job_field(self):
        resp = rpc.dispatch({
            "version": 1, "method": "enqueue_many",
            "params": {"jobs": [{
                "cmd": "run", "queue": "fast", "job_id": "J", "array_id": "batch",
                "hosts": ["eci10"], "priority": -100, "mps": True, "preempt": True,
                "payload_s3_uri": "s3://b/k.tar.gz", "payload_size_bytes": 42,
            }]},
        })
        self.assertTrue(resp["ok"], resp)
        item = queue_mod.load_queue()[0]
        self.assertEqual(item["array_id"], "batch")
        self.assertEqual(item["hosts"], ["eci10"])
        self.assertEqual(item["priority"], -100)
        self.assertTrue(item["mps"])
        self.assertEqual(item["payload_s3_uri"], "s3://b/k.tar.gz")
        self.assertEqual(item["payload_size_bytes"], 42)

    def test_enqueue_many_mints_ids_for_jobs_that_do_not_supply_one(self):
        resp = rpc.dispatch({
            "version": 1, "method": "enqueue_many",
            "params": {"jobs": [{"cmd": "a", "queue": "default"},
                                {"cmd": "b", "queue": "default"}]},
        })
        self.assertTrue(resp["ok"], resp)
        job_ids = [r["job_id"] for r in resp["result"]["results"]]
        self.assertTrue(all(job_ids))
        self.assertEqual(len(set(job_ids)), 2)

    def test_enqueue_many_names_the_overflow_rather_than_dropping_it(self):
        with patch.object(rpc, "MAX_ENQUEUE_BATCH", 2):
            resp = rpc.dispatch({
                "version": 1, "method": "enqueue_many",
                "params": {"jobs": [{"cmd": f"j{i}", "queue": "default"} for i in range(5)]},
            })
        self.assertTrue(resp["ok"], resp)
        self.assertEqual(resp["result"]["enqueued"], 2)
        self.assertEqual(resp["result"]["skipped"], [2, 3, 4])
        self.assertEqual(len(queue_mod.load_queue()), 2)

    def test_enqueue_many_rejects_a_non_list_or_empty_jobs_param(self):
        for jobs in (None, "nope", {}, []):
            resp = rpc.dispatch({
                "version": 1, "method": "enqueue_many", "params": {"jobs": jobs},
            })
            self.assertFalse(resp["ok"], jobs)
            self.assertEqual(resp["error"]["code"], "invalid_params")

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
