"""Tests for locating a job by id across the queue-host state files.

`lookup_job_state` sits under the `job_info` RPC, under `awsqe-client info
--queue-host local`, and under qdel's "where did that job go" message, so its
behavior is worth pinning down explicitly. The batch entry point
`lookup_job_states` must agree with it for every id, in every state — that is
what `BatchAgreementTests` checks.
"""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from awsqueueengine.shared import completion_state as completion_state_mod
from awsqueueengine.shared import failure_state as failure_state_mod
from awsqueueengine.shared import job_lookup
from awsqueueengine.shared import queue as queue_mod
from awsqueueengine.shared import running_state as running_state_mod


class _StateFixture(unittest.TestCase):
    """Redirect the queue-host state files to a temp dir."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        tmp = Path(self.tmpdir.name)
        self._originals = []
        for module, name, replacement in (
            (queue_mod, "QUEUE_FILE", tmp / "queue.json"),
            (running_state_mod, "RUNNING_FILE", tmp / "running.json"),
            (completion_state_mod, "COMPLETED_FILE", tmp / "completed.json"),
            (failure_state_mod, "FAILED_FILE", tmp / "failed.json"),
        ):
            self._originals.append((module, name, getattr(module, name)))
            setattr(module, name, replacement)

    def tearDown(self):
        for module, name, original in self._originals:
            setattr(module, name, original)
        self.tmpdir.cleanup()


class SingleLookupTests(_StateFixture):
    def test_empty_job_id_is_none(self):
        self.assertIsNone(job_lookup.lookup_job_state(""))
        self.assertIsNone(job_lookup.lookup_job_state(None))

    def test_unknown_job_id_is_none(self):
        self.assertIsNone(job_lookup.lookup_job_state("NOPE"))

    def test_queued_job_reports_one_based_position(self):
        queue_mod.save_queue([
            {"cmd": "first", "job_id": "A", "queue": "default"},
            {"cmd": "second", "job_id": "B", "queue": "gpu", "hosts": ["eci1", "eci2"]},
        ])
        state = job_lookup.lookup_job_state("B")
        self.assertEqual(state["status"], "queued")
        self.assertEqual(state["queue_position"], 2)
        self.assertEqual(state["queue"], "gpu")
        self.assertEqual(state["hosts_filter"], "eci1,eci2")
        self.assertEqual(state["cmd"], "second")

    def test_running_job_reports_host_and_formatted_start(self):
        running_state_mod.save_running_jobs({
            "eci7": {"cmd": "train.py", "job_id": "R1", "queue": "gpu",
                     "payload_remote_path": "/scratch/R1", "started_at": 1715537422.5},
        })
        state = job_lookup.lookup_job_state("R1")
        self.assertEqual(state["status"], "running")
        self.assertEqual(state["host"], "eci7")
        self.assertEqual(state["remote_payload_path"], "/scratch/R1")
        # Formatted in the *host's* local zone, not an epoch.
        self.assertRegex(state["started_at"], r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")

    def test_completed_job(self):
        completion_state_mod.save_completed_jobs([
            {"job_id": "C1", "host": "eci3", "cmd": "done.sh", "status": "completed",
             "dur": "00:10:00", "started_at": 1715537000.0, "finished_at": 1715537600.0},
        ])
        state = job_lookup.lookup_job_state("C1")
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["host"], "eci3")
        self.assertEqual(state["duration"], "00:10:00")

    def test_completed_job_without_exit_status_reports_unknown(self):
        completion_state_mod.save_completed_jobs([
            {"job_id": "U1", "host": "eci4", "cmd": "old.sh", "status": "unknown",
             "finished_at": 1715537600.0},
        ])
        self.assertEqual(job_lookup.lookup_job_state("U1")["status"], "unknown")

    def test_failed_job_carries_failure_fields(self):
        failure_state_mod.save_failed_jobs([
            {"job_id": "F1", "host": "eci9", "cmd": "oom.py", "exit_code": 137,
             "failure_reason": "out_of_memory", "failure_detail": "Killed",
             "finished_at": 1715537600.0},
        ])
        state = job_lookup.lookup_job_state("F1")
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state["failure_reason"], "out_of_memory")
        self.assertEqual(state["failure_detail"], "Killed")
        self.assertEqual(state["exit_code"], "137")

    def test_queue_wins_over_history_for_a_requeued_id(self):
        completion_state_mod.save_completed_jobs([
            {"job_id": "Q1", "host": "eci3", "finished_at": 1715537600.0},
        ])
        queue_mod.save_queue([{"cmd": "again", "job_id": "Q1"}])
        self.assertEqual(job_lookup.lookup_job_state("Q1")["status"], "queued")

    def test_first_queue_entry_wins_on_duplicate_job_id(self):
        queue_mod.save_queue([
            {"cmd": "first", "job_id": "D1"},
            {"cmd": "second", "job_id": "D1"},
        ])
        state = job_lookup.lookup_job_state("D1")
        self.assertEqual(state["queue_position"], 1)
        self.assertEqual(state["cmd"], "first")

    def test_newer_finished_at_wins_across_completed_and_failed(self):
        """A job that failed once and succeeded on retry appears in both files."""
        failure_state_mod.save_failed_jobs([
            {"job_id": "X1", "host": "eci1", "failure_reason": "nonzero_exit",
             "exit_code": 1, "finished_at": 100.0},
        ])
        completion_state_mod.save_completed_jobs([
            {"job_id": "X1", "host": "eci2", "status": "completed", "finished_at": 200.0},
        ])
        self.assertEqual(job_lookup.lookup_job_state("X1")["status"], "completed")

        # ...and the other way round: the retry failed.
        completion_state_mod.save_completed_jobs([
            {"job_id": "X1", "host": "eci2", "status": "completed", "finished_at": 100.0},
        ])
        failure_state_mod.save_failed_jobs([
            {"job_id": "X1", "host": "eci1", "failure_reason": "nonzero_exit",
             "exit_code": 1, "finished_at": 200.0},
        ])
        self.assertEqual(job_lookup.lookup_job_state("X1")["status"], "failed")

    def test_last_record_wins_within_one_history_file(self):
        completion_state_mod.save_completed_jobs([
            {"job_id": "L1", "host": "old", "status": "completed", "finished_at": 100.0},
            {"job_id": "L1", "host": "new", "status": "completed", "finished_at": 200.0},
        ])
        self.assertEqual(job_lookup.lookup_job_state("L1")["host"], "new")

    def test_queued_lookup_does_not_read_the_history_files(self):
        """Short-circuit matters: qdel's describe_missing_job runs on this path."""
        queue_mod.save_queue([{"cmd": "hi", "job_id": "S1"}])
        with patch.object(job_lookup, "load_completed_jobs") as completed, \
             patch.object(job_lookup, "load_failed_jobs") as failed:
            job_lookup.lookup_job_state("S1")
        completed.assert_not_called()
        failed.assert_not_called()


class ArrayIdPassthroughTests(_StateFixture):
    """`array_id` has to survive every state a job passes through, or the tag
    appears only while the job is queued and vanishes the moment it runs."""

    def test_queued_state_carries_the_array_id(self):
        queue_mod.save_queue([{"cmd": "a", "job_id": "A", "array_id": "ffpopt-IDC"}])
        self.assertEqual(job_lookup.lookup_job_state("A")["array_id"], "ffpopt-IDC")

    def test_running_state_carries_the_array_id(self):
        running_state_mod.save_running_jobs({
            "eci5": {"cmd": "a", "job_id": "A", "array_id": "ffpopt-IDC",
                     "started_at": 1715537422.5},
        })
        self.assertEqual(job_lookup.lookup_job_state("A")["array_id"], "ffpopt-IDC")

    def test_finished_state_carries_the_array_id(self):
        completion_state_mod.save_completed_jobs([
            {"job_id": "A", "host": "eci5", "status": "completed", "exit_code": 0,
             "finished_at": 100.0, "array_id": "ffpopt-IDC"},
        ])
        self.assertEqual(job_lookup.lookup_job_state("A")["array_id"], "ffpopt-IDC")

    def test_failed_state_carries_the_array_id(self):
        failure_state_mod.save_failed_jobs([
            {"job_id": "A", "host": "eci5", "status": "failed", "exit_code": 137,
             "finished_at": 100.0, "array_id": "ffpopt-IDC"},
        ])
        self.assertEqual(job_lookup.lookup_job_state("A")["array_id"], "ffpopt-IDC")

    def test_an_untagged_job_reports_an_empty_tag_rather_than_omitting_it(self):
        queue_mod.save_queue([{"cmd": "a", "job_id": "A"}])
        self.assertEqual(job_lookup.lookup_job_state("A")["array_id"], "")


class RunningMembersOfArrayTests(_StateFixture):
    def test_finds_only_that_batch_in_host_order(self):
        running_state_mod.save_running_jobs({
            "eci7": {"cmd": "a", "job_id": "R2", "array_id": "ffpopt-IDC"},
            "eci5": {"cmd": "a", "job_id": "R1", "array_id": "ffpopt-IDC"},
            "eci6": {"cmd": "b", "job_id": "R3", "array_id": "other"},
            "eci8": {"cmd": "c", "job_id": "R4"},
        })
        self.assertEqual(
            job_lookup.running_members_of_array("ffpopt-IDC"),
            [{"host": "eci5", "job_id": "R1"}, {"host": "eci7", "job_id": "R2"}],
        )

    def test_matching_is_case_insensitive(self):
        running_state_mod.save_running_jobs({
            "eci5": {"cmd": "a", "job_id": "R1", "array_id": "ffpopt-IDC"},
        })
        self.assertEqual(len(job_lookup.running_members_of_array("FFPOPT-idc")), 1)

    def test_empty_or_missing_name_matches_nothing(self):
        running_state_mod.save_running_jobs({"eci5": {"cmd": "a", "job_id": "R1"}})
        self.assertEqual(job_lookup.running_members_of_array(""), [])
        self.assertEqual(job_lookup.running_members_of_array(None), [])
        self.assertEqual(job_lookup.running_members_of_array("nope"), [])


class BatchAgreementTests(_StateFixture):
    """`lookup_job_states` must be indistinguishable from N `lookup_job_state`s."""

    def _populate(self):
        queue_mod.save_queue([
            {"cmd": "q-one", "job_id": "Q1", "queue": "default"},
            {"cmd": "q-two", "job_id": "Q2", "queue": "gpu", "hosts": ["eci1"]},
        ])
        running_state_mod.save_running_jobs({
            "eci7": {"cmd": "r-one", "job_id": "R1", "started_at": 1715537422.5},
            "eci8": {"cmd": "r-two", "job_id": "R2", "started_at": 1715537999.0},
        })
        completion_state_mod.save_completed_jobs([
            {"job_id": "C1", "host": "eci3", "status": "completed", "dur": "00:01:00",
             "started_at": 1715530000.0, "finished_at": 1715530060.0},
            {"job_id": "U1", "host": "eci4", "status": "unknown", "finished_at": 1715530060.0},
            {"job_id": "X1", "host": "eci5", "status": "completed", "finished_at": 200.0},
        ])
        failure_state_mod.save_failed_jobs([
            {"job_id": "F1", "host": "eci9", "exit_code": 137,
             "failure_reason": "out_of_memory", "finished_at": 1715530060.0},
            {"job_id": "X1", "host": "eci6", "exit_code": 1,
             "failure_reason": "nonzero_exit", "finished_at": 100.0},
        ])
        return ["Q1", "Q2", "R1", "R2", "C1", "U1", "F1", "X1", "GONE", ""]

    def test_batch_matches_single_for_every_state(self):
        job_ids = self._populate()
        batch = job_lookup.lookup_job_states(job_ids)
        for job_id in job_ids:
            if not job_id:
                self.assertNotIn(job_id, batch)
                continue
            self.assertEqual(batch[job_id], job_lookup.lookup_job_state(job_id),
                             msg=f"batch and single disagree for {job_id!r}")

    def test_every_requested_id_is_present_with_explicit_none(self):
        self._populate()
        batch = job_lookup.lookup_job_states(["Q1", "GONE"])
        self.assertEqual(set(batch), {"Q1", "GONE"})
        self.assertIsNone(batch["GONE"])

    def test_empty_input(self):
        self.assertEqual(job_lookup.lookup_job_states([]), {})
        self.assertEqual(job_lookup.lookup_job_states(None), {})
        self.assertEqual(job_lookup.lookup_job_states(["", None]), {})

    def test_duplicate_ids_collapse(self):
        self._populate()
        batch = job_lookup.lookup_job_states(["Q1", "Q1"])
        self.assertEqual(set(batch), {"Q1"})

    def test_all_queued_batch_does_not_read_the_history_files(self):
        queue_mod.save_queue([
            {"cmd": "a", "job_id": "A"},
            {"cmd": "b", "job_id": "B"},
        ])
        with patch.object(job_lookup, "load_completed_jobs") as completed, \
             patch.object(job_lookup, "load_failed_jobs") as failed:
            job_lookup.lookup_job_states(["A", "B"])
        completed.assert_not_called()
        failed.assert_not_called()


if __name__ == "__main__":
    unittest.main()
