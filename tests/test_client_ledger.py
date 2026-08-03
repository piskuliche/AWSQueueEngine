"""Tests for the client-side tracked-job ledger (~/.awsqe/client/jobs.json)."""
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

from awsqueueengine.client import ledger as ledger_mod
from awsqueueengine.client.ledger import (
    LedgerSelectionError,
    apply_state,
    apply_states,
    filter_records,
    find_record,
    forget,
    load_ledger,
    mark_status,
    merge_state,
    prune_records,
    record_submission,
    save_ledger,
)


class _LedgerFixture(unittest.TestCase):
    """Point LEDGER_PATH at a temp dir, the way test_client_config patches CONFIG_PATH."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.path = Path(self.tmpdir.name) / "jobs.json"
        self._original = ledger_mod.LEDGER_PATH
        ledger_mod.LEDGER_PATH = self.path

    def tearDown(self):
        ledger_mod.LEDGER_PATH = self._original
        self.tmpdir.cleanup()

    def _submit(self, job_id, *, queue_host="qh", submitted_at=1000.0, **kwargs):
        return record_submission(
            job_id=job_id, queue_host=queue_host, submitted_at=submitted_at, **kwargs
        )


class LoadSaveTests(_LedgerFixture):
    def test_missing_file_is_empty_and_silent(self):
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            self.assertEqual(load_ledger(), [])
        self.assertEqual(stderr.getvalue(), "")

    def test_round_trip_keeps_oldest_first(self):
        self._submit("A", submitted_at=100.0)
        self._submit("B", submitted_at=200.0)
        self.assertEqual([r["job_id"] for r in load_ledger()], ["A", "B"])

    def test_submission_records_the_expected_fields(self):
        self._submit("A", queue="gpu", cmd="python t.py", payload="/data/run",
                     payload_s3_uri="s3://b/k.tar.gz")
        record = load_ledger()[0]
        self.assertEqual(record["status"], "submitted")
        self.assertEqual(record["queue_host"], "qh")
        self.assertEqual(record["queue"], "gpu")
        self.assertEqual(record["cmd"], "python t.py")
        self.assertEqual(record["payload"], "/data/run")
        self.assertEqual(record["payload_s3_uri"], "s3://b/k.tar.gz")
        self.assertEqual(record["submitted_at"], 1000.0)

    def test_file_is_a_versioned_envelope(self):
        self._submit("A")
        data = json.loads(self.path.read_text())
        self.assertEqual(data["version"], ledger_mod.LEDGER_VERSION)
        self.assertEqual([r["job_id"] for r in data["jobs"]], ["A"])

    def test_bare_list_is_accepted(self):
        self.path.write_text(json.dumps([{"job_id": "A", "submitted_at": 5.0}]))
        self.assertEqual([r["job_id"] for r in load_ledger()], ["A"])

    def test_unknown_record_fields_survive_a_save(self):
        """An older client must not strip a field a newer one wrote."""
        self.path.write_text(json.dumps({
            "version": 1,
            "jobs": [{"job_id": "A", "submitted_at": 5.0, "future_field": "keep me"}],
        }))
        mark_status(["A"], "deleted")
        self.assertEqual(load_ledger()[0]["future_field"], "keep me")

    def test_records_without_a_job_id_are_dropped(self):
        self.path.write_text(json.dumps({"version": 1, "jobs": [
            {"job_id": "", "submitted_at": 1.0}, {"submitted_at": 2.0}, {"job_id": "A"},
        ]}))
        self.assertEqual([r["job_id"] for r in load_ledger()], ["A"])

    def test_bad_submitted_at_becomes_zero_rather_than_crashing_a_sort(self):
        self.path.write_text(json.dumps({"version": 1, "jobs": [
            {"job_id": "A", "submitted_at": "not a number"},
            {"job_id": "B", "submitted_at": True},
        ]}))
        self.assertEqual([r["submitted_at"] for r in load_ledger()], [0.0, 0.0])

    def test_corrupt_file_warns_and_reads_empty(self):
        self.path.write_text("{not json")
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            self.assertEqual(load_ledger(), [])
        self.assertIn("failed to parse", stderr.getvalue())

    def test_corrupt_file_is_quarantined_before_the_next_write(self):
        self.path.write_text("{not json")
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            self._submit("A")
        self.assertTrue(self.path.with_suffix(".json.corrupt").exists())
        self.assertEqual(self.path.with_suffix(".json.corrupt").read_text(), "{not json")
        self.assertEqual([r["job_id"] for r in load_ledger()], ["A"])

    def test_no_write_when_nothing_changed(self):
        self._submit("A")
        before = self.path.read_text()
        mtime = self.path.stat().st_mtime_ns
        self.assertEqual(apply_states({"A": {"status": "submitted"}}), 0)
        self.assertEqual(mark_status(["A"], "submitted"), 0)
        self.assertEqual(self.path.read_text(), before)
        self.assertEqual(self.path.stat().st_mtime_ns, mtime)

    def test_no_temp_files_are_left_behind(self):
        """The lock file persists by design; a half-written .tmp must not."""
        self._submit("A")
        leftovers = [p.name for p in self.path.parent.iterdir() if ".tmp" in p.name]
        self.assertEqual(leftovers, [])


class PruneTests(_LedgerFixture):
    def _records(self, count, status):
        return [{"job_id": f"J{i}", "submitted_at": float(i), "status": status}
                for i in range(count)]

    def test_under_the_cap_is_untouched(self):
        records = self._records(5, "completed")
        self.assertEqual(prune_records(records), records)

    def test_oldest_terminal_records_are_dropped_and_the_newest_kept(self):
        records = self._records(ledger_mod.MAX_TRACKED_RECORDS + 10, "completed")
        pruned = prune_records(records)
        self.assertEqual(len(pruned), ledger_mod.MAX_TRACKED_RECORDS)
        # The newest must survive; a naive records[:MAX] gets this backwards.
        self.assertEqual(pruned[-1]["job_id"], records[-1]["job_id"])
        self.assertEqual(pruned[0]["job_id"], "J10")

    def test_non_terminal_records_are_never_evicted(self):
        records = self._records(ledger_mod.MAX_TRACKED_RECORDS + 10, "completed")
        records[0]["status"] = "running"
        records[1]["status"] = "queued"
        pruned = prune_records(records)
        kept = {r["job_id"] for r in pruned}
        self.assertIn("J0", kept)
        self.assertIn("J1", kept)
        self.assertEqual(len(pruned), ledger_mod.MAX_TRACKED_RECORDS)

    def test_all_non_terminal_stays_above_the_cap_rather_than_losing_live_jobs(self):
        records = self._records(ledger_mod.MAX_TRACKED_RECORDS + 10, "running")
        self.assertEqual(len(prune_records(records)), len(records))

    def test_save_prunes(self):
        save_ledger(self._records(ledger_mod.MAX_TRACKED_RECORDS + 10, "completed"), self.path)
        self.assertEqual(len(load_ledger()), ledger_mod.MAX_TRACKED_RECORDS)


class ConcurrencyTests(_LedgerFixture):
    def test_apply_states_does_not_lose_a_record_appended_after_its_snapshot(self):
        """The refresh re-reads inside the lock, so a racing submit survives."""
        self._submit("A", submitted_at=100.0)
        stale_snapshot = load_ledger()          # what a refresh would have started from
        self._submit("B", submitted_at=200.0)   # ...a submit lands meanwhile

        apply_states({r["job_id"]: {"status": "completed"} for r in stale_snapshot})

        by_id = {r["job_id"]: r for r in load_ledger()}
        self.assertEqual(set(by_id), {"A", "B"})
        self.assertEqual(by_id["A"]["status"], "completed")
        self.assertEqual(by_id["B"]["status"], "submitted")


class MergeStateTests(unittest.TestCase):
    def _record(self, **kwargs):
        base = {"job_id": "A", "submitted_at": 100.0, "queue_host": "qh",
                "payload": "/data/run", "status": "submitted", "updated_at": 0.0}
        base.update(kwargs)
        return base

    def test_host_fields_are_merged(self):
        merged = merge_state(self._record(), {"status": "running", "host": "eci7"}, now=5.0)
        self.assertEqual(merged["status"], "running")
        self.assertEqual(merged["host"], "eci7")
        self.assertEqual(merged["updated_at"], 5.0)

    def test_empty_values_never_overwrite(self):
        record = self._record(host="eci7", queue="gpu")
        merged = merge_state(record, {"status": "running", "host": "", "queue": None})
        self.assertEqual(merged["host"], "eci7")
        self.assertEqual(merged["queue"], "gpu")

    def test_client_owned_fields_are_never_overwritten(self):
        merged = merge_state(
            self._record(),
            {"status": "running", "job_id": "OTHER", "submitted_at": 999.0,
             "queue_host": "elsewhere", "payload": "/wrong"},
        )
        self.assertEqual(merged["job_id"], "A")
        self.assertEqual(merged["submitted_at"], 100.0)
        self.assertEqual(merged["queue_host"], "qh")
        self.assertEqual(merged["payload"], "/data/run")

    def test_queue_is_host_owned_and_does_update(self):
        merged = merge_state(self._record(queue="default"), {"status": "queued", "queue": "gpu"})
        self.assertEqual(merged["queue"], "gpu")

    def test_failure_fields_are_kept_while_failed(self):
        merged = merge_state(self._record(), {
            "status": "failed", "failure_reason": "out_of_memory",
            "failure_detail": "Killed", "exit_code": "137",
        })
        self.assertEqual(merged["failure_reason"], "out_of_memory")
        self.assertEqual(merged["exit_code"], "137")

    def test_a_successful_retry_clears_the_previous_failure_fields(self):
        record = self._record(status="failed", failure_reason="out_of_memory",
                              failure_detail="Killed", exit_code="137")
        merged = merge_state(record, {"status": "completed"})
        self.assertEqual(merged["status"], "completed")
        for key in ("failure_reason", "failure_detail", "exit_code"):
            self.assertNotIn(key, merged)

    def test_none_state_means_missing_and_is_timestamped(self):
        merged = merge_state(self._record(), None, now=5.0)
        self.assertEqual(merged["status"], "missing")
        self.assertEqual(merged["missing_since"], 5.0)

    def test_missing_since_is_not_bumped_on_a_repeat_miss(self):
        first = merge_state(self._record(), None, now=5.0)
        second = merge_state(first, None, now=99.0)
        self.assertEqual(second["missing_since"], 5.0)
        self.assertEqual(second, first)   # nothing changed, so no write is triggered

    def test_resolving_out_of_missing_clears_missing_since(self):
        missing = merge_state(self._record(), None, now=5.0)
        merged = merge_state(missing, {"status": "running", "host": "eci7"}, now=9.0)
        self.assertNotIn("missing_since", merged)
        self.assertEqual(merged["status"], "running")

    def test_no_change_leaves_updated_at_alone(self):
        record = self._record(status="running", host="eci7", updated_at=1.0)
        merged = merge_state(record, {"status": "running", "host": "eci7"}, now=99.0)
        self.assertEqual(merged["updated_at"], 1.0)
        self.assertEqual(merged, record)


class ApplyAndMarkTests(_LedgerFixture):
    def test_apply_state_updates_a_tracked_job(self):
        self._submit("A")
        self.assertTrue(apply_state("A", {"status": "running", "host": "eci7"}))
        self.assertEqual(load_ledger()[0]["status"], "running")

    def test_apply_state_never_inserts_an_untracked_job(self):
        self._submit("A")
        self.assertFalse(apply_state("B", {"status": "running"}))
        self.assertEqual([r["job_id"] for r in load_ledger()], ["A"])

    def test_mark_status_sets_deleted_and_drops_the_stale_queue_position(self):
        self._submit("A")
        apply_state("A", {"status": "queued", "queue_position": 3})
        self.assertEqual(mark_status(["A"], "deleted"), 1)
        record = load_ledger()[0]
        self.assertEqual(record["status"], "deleted")
        self.assertNotIn("queue_position", record)

    def test_mark_status_ignores_untracked_ids(self):
        self._submit("A")
        self.assertEqual(mark_status(["B", "C"], "deleted"), 0)


class ForgetTests(_LedgerFixture):
    def test_forget_by_id(self):
        self._submit("A")
        self._submit("B")
        self.assertEqual(forget(job_ids=["A"]), 1)
        self.assertEqual([r["job_id"] for r in load_ledger()], ["B"])

    def test_forget_before_a_cutoff(self):
        self._submit("old", submitted_at=100.0)
        self._submit("new", submitted_at=300.0)
        self.assertEqual(forget(before=200.0), 1)
        self.assertEqual([r["job_id"] for r in load_ledger()], ["new"])

    def test_forget_with_no_selector_is_a_noop(self):
        self._submit("A")
        self.assertEqual(forget(), 0)
        self.assertEqual(len(load_ledger()), 1)


class FindRecordTests(_LedgerFixture):
    def test_exact_match(self):
        self._submit("20260730-141530-a1b2c3")
        self.assertEqual(find_record("20260730-141530-a1b2c3")["job_id"],
                         "20260730-141530-a1b2c3")

    def test_unique_prefix(self):
        self._submit("20260730-141530-a1b2c3")
        self.assertEqual(find_record("20260730-1415")["job_id"], "20260730-141530-a1b2c3")

    def test_prefix_is_case_insensitive(self):
        self._submit("20260730-141530-A1B2C3")
        self.assertIsNotNone(find_record("20260730-141530-a1b2c3"))

    def test_ambiguous_prefix_raises_and_names_the_candidates(self):
        self._submit("20260730-141530-a1b2c3")
        self._submit("20260730-141530-d4e5f6")
        with self.assertRaises(LedgerSelectionError) as ctx:
            find_record("20260730-1415")
        self.assertEqual(len(ctx.exception.candidates), 2)

    def test_exact_match_wins_over_being_a_prefix_of_another(self):
        self._submit("ABC")
        self._submit("ABCDEF")
        self.assertEqual(find_record("ABC")["job_id"], "ABC")

    def test_unknown_is_none(self):
        self._submit("A")
        self.assertIsNone(find_record("ZZZ"))
        self.assertIsNone(find_record(""))


class FilterRecordsTests(unittest.TestCase):
    def setUp(self):
        self.records = [
            {"job_id": "A", "submitted_at": 100.0, "status": "completed"},
            {"job_id": "B", "submitted_at": 200.0, "status": "running"},
            {"job_id": "C", "submitted_at": 300.0, "status": "failed"},
        ]

    def _ids(self, **kwargs):
        return [r["job_id"] for r in filter_records(self.records, **kwargs)]

    def test_default_is_newest_first(self):
        self.assertEqual(self._ids(), ["C", "B", "A"])

    def test_status_filter(self):
        self.assertEqual(self._ids(statuses={"running"}), ["B"])
        self.assertEqual(self._ids(statuses={"running", "failed"}), ["C", "B"])

    def test_queue_filter(self):
        for record, queue in zip(self.records, ("default", "gpu", "gpu")):
            record["queue"] = queue
        self.assertEqual(self._ids(queues={"gpu"}), ["C", "B"])
        self.assertEqual(self._ids(queues={"default", "gpu"}), ["C", "B", "A"])
        self.assertEqual(self._ids(queues={"nope"}), [])

    def test_queue_filter_is_case_insensitive(self):
        self.records[0]["queue"] = "Zeke-Queue"
        self.assertEqual(self._ids(queues={"zeke-queue"}), ["A"])

    def test_queue_filter_tolerates_records_with_no_queue(self):
        self.assertEqual(self._ids(queues={"gpu"}), [])

    def test_queue_and_status_filters_compose(self):
        for record, queue in zip(self.records, ("gpu", "gpu", "default")):
            record["queue"] = queue
        self.assertEqual(self._ids(queues={"gpu"}, statuses={"running"}), ["B"])

    def test_since_is_inclusive_and_until_is_exclusive(self):
        self.assertEqual(self._ids(since=200.0), ["C", "B"])
        self.assertEqual(self._ids(until=200.0), ["A"])
        self.assertEqual(self._ids(since=200.0, until=300.0), ["B"])

    def test_limit_applies_after_sorting(self):
        self.assertEqual(self._ids(limit=2), ["C", "B"])

    def test_zero_or_negative_limit_means_no_limit(self):
        self.assertEqual(self._ids(limit=0), ["C", "B", "A"])
        self.assertEqual(self._ids(limit=None), ["C", "B", "A"])
        # A negative would otherwise slice the tail off.
        self.assertEqual(self._ids(limit=-1), ["C", "B", "A"])

    def test_input_is_not_mutated(self):
        filter_records(self.records, limit=1)
        self.assertEqual([r["job_id"] for r in self.records], ["A", "B", "C"])


if __name__ == "__main__":
    unittest.main()
