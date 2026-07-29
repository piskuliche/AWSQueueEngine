import unittest
from datetime import date
from pathlib import Path
import tempfile
from unittest.mock import patch

from awsqueueengine.host.monitor import (
    _build_job_fail_alert_body,
    _build_completed_job_record,
    _build_failed_job_record,
    _initial_alert_runtime_state,
    _launch_job_on_host,
    _prune_running_jobs_for_status,
    _reset_queue_alert_state,
    _select_preempt_target,
    _should_send_alert,
    _should_send_daily_summary,
    _should_send_empty_queue_alert,
    _should_send_low_queue_alert,
    load_hosts_from_file,
    monitor_loop,
)


def _outcome(exit_code=0, log_tail="", found=True, error=""):
    return {"exit_code": exit_code, "log_tail": log_tail, "found": found, "error": error}


class MonitorRunningStatePruneTests(unittest.TestCase):
    def test_unreachable_host_keeps_running_metadata(self):
        running_jobs = {
            "eci5": {"cmd": "run-a"},
            "eci6": {"cmd": "run-b"},
        }
        status_rows = [
            {"host": "eci5", "reachable": False, "pid": None},
            {"host": "eci6", "reachable": True, "pid": "123"},
        ]

        changed, completed_records, failed_records = _prune_running_jobs_for_status(
            running_jobs, status_rows, fetch_outcome=lambda host, tag: _outcome()
        )

        self.assertFalse(changed)
        self.assertEqual(completed_records, [])
        self.assertEqual(failed_records, [])
        self.assertEqual(set(running_jobs), {"eci5", "eci6"})

    def test_reachable_idle_host_is_pruned(self):
        running_jobs = {
            "eci5": {"cmd": "run-a", "priority": 10, "preempt": False, "hosts": None, "started_at": 100.0},
            "eci6": {"cmd": "run-b"},
        }
        status_rows = [
            {"host": "eci5", "reachable": True, "pid": None},
            {"host": "eci6", "reachable": True, "pid": "123"},
        ]

        with patch("awsqueueengine.host.monitor.time.time", return_value=160.0):
            changed, completed_records, failed_records = _prune_running_jobs_for_status(
                running_jobs, status_rows, fetch_outcome=lambda host, tag: _outcome()
            )

        self.assertTrue(changed)
        self.assertEqual(set(running_jobs), {"eci6"})
        self.assertEqual(failed_records, [])
        self.assertEqual(len(completed_records), 1)
        self.assertEqual(completed_records[0]["host"], "eci5")
        self.assertEqual(completed_records[0]["dur"], "00:01:00")
        self.assertEqual(completed_records[0]["duration_seconds"], 60)
        self.assertEqual(completed_records[0]["cmd"], "run-a")
        self.assertEqual(completed_records[0]["status"], "completed")
        self.assertEqual(completed_records[0]["exit_code"], 0)

    def test_nonzero_exit_is_recorded_as_a_failure(self):
        running_jobs = {"eci5": {"cmd": "run-a", "job_id": "tag-1", "started_at": 100.0}}
        status_rows = [{"host": "eci5", "reachable": True, "pid": None}]

        with patch("awsqueueengine.host.monitor.time.time", return_value=105.0):
            changed, completed_records, failed_records = _prune_running_jobs_for_status(
                running_jobs,
                status_rows,
                fetch_outcome=lambda host, tag: _outcome(1, "python: run.py: No such file"),
            )

        self.assertTrue(changed)
        self.assertEqual(completed_records, [])
        self.assertEqual(len(failed_records), 1)
        record = failed_records[0]
        self.assertEqual(record["host"], "eci5")
        self.assertEqual(record["job_id"], "tag-1")
        self.assertEqual(record["status"], "failed")
        self.assertEqual(record["exit_code"], 1)
        self.assertEqual(record["failure_reason"], "nonzero_exit")
        self.assertEqual(record["dur"], "00:00:05")
        self.assertIn("No such file", record["failure_detail"])

    def test_tracked_job_without_exit_status_is_recorded_as_a_failure(self):
        running_jobs = {"eci5": {"cmd": "run-a", "job_id": "tag-2", "exit_status_tracked": True}}
        status_rows = [{"host": "eci5", "reachable": True, "pid": None}]

        _changed, completed_records, failed_records = _prune_running_jobs_for_status(
            running_jobs,
            status_rows,
            fetch_outcome=lambda host, tag: _outcome(None, "", found=False, error="no exit status recorded on host"),
        )

        self.assertEqual(completed_records, [])
        self.assertEqual(len(failed_records), 1)
        self.assertEqual(failed_records[0]["failure_reason"], "no_exit_status")
        self.assertIsNone(failed_records[0]["exit_code"])

    def test_untracked_job_without_exit_status_is_unknown_not_failed(self):
        # Jobs already running when the monitor was upgraded never got the
        # exit-status wrapper, so a missing .rc file proves nothing. Reporting
        # those as failures marked clean 7-hour runs as broken in production.
        running_jobs = {"eci5": {"cmd": "run-a", "job_id": "tag-old", "started_at": 100.0}}
        status_rows = [{"host": "eci5", "reachable": True, "pid": None}]

        with patch("awsqueueengine.host.monitor.time.time", return_value=25300.0):
            _changed, completed_records, failed_records = _prune_running_jobs_for_status(
                running_jobs,
                status_rows,
                fetch_outcome=lambda host, tag: _outcome(None, "", found=False, error="no exit status recorded on host"),
            )

        self.assertEqual(failed_records, [])
        self.assertEqual(len(completed_records), 1)
        self.assertEqual(completed_records[0]["status"], "unknown")
        self.assertIsNone(completed_records[0]["exit_code"])
        self.assertEqual(completed_records[0]["dur"], "07:00:00")

    def test_untracked_job_with_a_real_nonzero_exit_still_fails(self):
        # Absence of a status is unknowable; a status we can read is not.
        running_jobs = {"eci5": {"cmd": "run-a", "job_id": "tag-old"}}
        status_rows = [{"host": "eci5", "reachable": True, "pid": None}]

        _changed, completed_records, failed_records = _prune_running_jobs_for_status(
            running_jobs, status_rows, fetch_outcome=lambda host, tag: _outcome(1, "boom")
        )

        self.assertEqual(completed_records, [])
        self.assertEqual(len(failed_records), 1)
        self.assertEqual(failed_records[0]["exit_code"], 1)

    def test_outcome_fetch_error_still_records_the_job(self):
        running_jobs = {"eci5": {"cmd": "run-a", "job_id": "tag-3", "exit_status_tracked": True}}
        status_rows = [{"host": "eci5", "reachable": True, "pid": None}]

        def boom(host, tag):
            raise OSError("ssh exploded")

        _changed, completed_records, failed_records = _prune_running_jobs_for_status(
            running_jobs, status_rows, fetch_outcome=boom
        )

        self.assertEqual(completed_records, [])
        self.assertEqual(len(failed_records), 1)
        self.assertIn("ssh exploded", failed_records[0]["failure_detail"])

    def test_missing_host_in_status_keeps_running_metadata(self):
        running_jobs = {"eci9": {"cmd": "run-z"}}
        status_rows = [{"host": "eci8", "reachable": True, "pid": None}]

        changed, completed_records, failed_records = _prune_running_jobs_for_status(
            running_jobs, status_rows, fetch_outcome=lambda host, tag: _outcome()
        )

        self.assertFalse(changed)
        self.assertEqual(completed_records, [])
        self.assertEqual(failed_records, [])
        self.assertEqual(set(running_jobs), {"eci9"})

    def test_build_failed_job_record_keeps_job_metadata_and_log_tail(self):
        record = _build_failed_job_record(
            "eci8",
            {"cmd": "pmemd.cuda", "job_id": "tag-9", "queue": "gpu", "started_at": 10.0},
            finished_at=25.0,
            outcome=_outcome(137, "slurmstepd: Killed\nout of memory"),
        )
        self.assertEqual(record["status"], "failed")
        self.assertEqual(record["exit_code"], 137)
        self.assertEqual(record["failure_reason"], "out_of_memory")
        self.assertEqual(record["queue"], "gpu")
        self.assertEqual(record["dur"], "00:00:15")
        self.assertEqual(record["failed_at"], 25.0)
        self.assertIn("out of memory", record["log_tail"])

    def test_build_completed_job_record_uses_qstat_payload_selection(self):
        record = _build_completed_job_record(
            "eci8",
            {
                "cmd": "python run.py",
                "priority": 7,
                "preempt": True,
                "hosts": ["eci8"],
                "payload": "/tmp/local",
                "payload_remote_path": "/remote/payload",
                "payload_s3_uri": "s3://bucket/payload.tar.gz",
                "payload_size_bytes": 123,
                "started_at": 10.0,
            },
            finished_at=40.0,
        )
        self.assertEqual(record["host"], "eci8")
        self.assertEqual(record["dur"], "00:00:30")
        self.assertEqual(record["payload"], "/remote/payload")
        self.assertEqual(record["payload_local_path"], "/tmp/local")
        self.assertEqual(record["payload_remote_path"], "/remote/payload")
        self.assertEqual(record["payload_s3_uri"], "s3://bucket/payload.tar.gz")
        self.assertEqual(record["payload_size_bytes"], 123)
        self.assertEqual(record["priority"], 7)
        self.assertTrue(record["preempt"])
        self.assertEqual(record["hosts"], ["eci8"])
        self.assertEqual(record["cmd"], "python run.py")


class MonitorFastFinishTests(unittest.TestCase):
    """A job whose process is gone by the pid check may have succeeded."""

    FAST_EXIT = {"ok": False, "err": "pidfile present but process not running",
                 "reason": "job", "tag": "tag-1", "payload": "/scratch/p"}

    def _launch(self, outcome):
        recorded = {"completed": [], "failed": []}
        with patch("awsqueueengine.host.monitor.submit_to_host", return_value=dict(self.FAST_EXIT)), patch(
            "awsqueueengine.host.monitor.fetch_job_outcome", return_value=outcome
        ), patch(
            "awsqueueengine.host.monitor.append_completed_records",
            side_effect=lambda r: recorded["completed"].extend(r),
        ), patch(
            "awsqueueengine.host.monitor.append_failed_records",
            side_effect=lambda r: recorded["failed"].extend(r),
        ):
            result = _launch_job_on_host("eci5", {"cmd": "run.sh", "job_id": "tag-1"}, {})
        return result, recorded

    def test_exit_zero_before_pid_check_is_recorded_as_completed(self):
        # A job with nothing left to do exits 0 in under a second and looks
        # identical to a crash from the launcher's point of view. Recording it
        # as a failure marked successful production runs as broken.
        result, recorded = self._launch(_outcome(0, "all nodes completed"))

        self.assertEqual(recorded["failed"], [])
        self.assertEqual(len(recorded["completed"]), 1)
        self.assertEqual(recorded["completed"][0]["status"], "completed")
        self.assertEqual(recorded["completed"][0]["exit_code"], 0)
        # Must not look like a start failure: no alert, no dispatch stall.
        self.assertTrue(result["finished_immediately"])
        self.assertIsNone(result["reason"])

    def test_nonzero_exit_before_pid_check_is_still_a_failure(self):
        result, recorded = self._launch(_outcome(1, "Traceback (most recent call last):"))

        self.assertEqual(recorded["completed"], [])
        self.assertEqual(len(recorded["failed"]), 1)
        self.assertEqual(recorded["failed"][0]["failure_reason"], "python_exception")
        self.assertFalse(result.get("finished_immediately"))
        self.assertEqual(result["reason"], "job")

    def test_missing_status_after_launch_is_a_failure(self):
        # We just launched it ourselves, so the wrapper was definitely applied:
        # a missing status here really does mean the job died.
        _result, recorded = self._launch(_outcome(None, "", found=False, error="no exit status recorded on host"))

        self.assertEqual(recorded["completed"], [])
        self.assertEqual(recorded["failed"][0]["failure_reason"], "no_exit_status")


class MonitorLaunchTrackingTests(unittest.TestCase):
    def test_launch_marks_the_running_job_as_exit_status_tracked(self):
        running_jobs = {}
        with patch(
            "awsqueueengine.host.monitor.submit_to_host",
            return_value={"ok": True, "tag": "tag-1", "pid": "123", "payload": "/scratch/p"},
        ), patch("awsqueueengine.host.monitor.save_running_jobs"), patch(
            "awsqueueengine.host.monitor.write_run_info"
        ):
            result = _launch_job_on_host("eci5", {"cmd": "run.sh", "job_id": "tag-1"}, running_jobs)

        self.assertTrue(result["launched"])
        # Without this flag the prune step can't tell a killed job from one that
        # predates exit-status tracking.
        self.assertTrue(running_jobs["eci5"]["exit_status_tracked"])


class MonitorPreemptTargetTests(unittest.TestCase):
    def test_preempt_requires_strictly_higher_priority_than_running_job(self):
        queue_items = [
            {"cmd": "same-priority", "priority": 100, "preempt": True, "hosts": ["eci1"]},
            {"cmd": "lower-priority", "priority": 50, "preempt": True, "hosts": ["eci2"]},
        ]
        running_jobs = {
            "eci1": {"cmd": "active-a", "priority": 100},
            "eci2": {"cmd": "active-b", "priority": 100},
        }

        target = _select_preempt_target(queue_items, ["eci1", "eci2"], running_jobs)

        self.assertEqual(target, (None, None, None))

    def test_preempt_selects_lower_priority_victim_on_eligible_host(self):
        queue_items = [
            {"cmd": "urgent", "priority": 100, "preempt": True, "hosts": ["eci1", "eci2"]},
        ]
        running_jobs = {
            "eci1": {"cmd": "production-a", "priority": -100},
            "eci2": {"cmd": "production-b", "priority": 0},
        }

        queue_idx, item, victim = _select_preempt_target(queue_items, ["eci1", "eci2"], running_jobs)

        self.assertEqual(queue_idx, 0)
        self.assertEqual(item["cmd"], "urgent")
        self.assertEqual(victim, "eci1")


class MonitorAlertDecisionTests(unittest.TestCase):
    def test_low_queue_alert_triggers_only_between_1_and_9(self):
        self.assertTrue(_should_send_low_queue_alert(9, already_sent=False))
        self.assertFalse(_should_send_low_queue_alert(10, already_sent=False))
        self.assertFalse(_should_send_low_queue_alert(0, already_sent=False))
        self.assertFalse(_should_send_low_queue_alert(5, already_sent=True))

    def test_empty_queue_alert_triggers_only_for_zero(self):
        self.assertTrue(_should_send_empty_queue_alert(0, already_sent=False))
        self.assertFalse(_should_send_empty_queue_alert(1, already_sent=False))
        self.assertFalse(_should_send_empty_queue_alert(0, already_sent=True))

    def test_daily_summary_requires_new_date(self):
        class FakeDateTime:
            def __init__(self, day):
                self._day = day

            def date(self):
                return self._day

        self.assertTrue(_should_send_daily_summary(date(2026, 3, 8), FakeDateTime(date(2026, 3, 9))))
        self.assertFalse(_should_send_daily_summary(date(2026, 3, 9), FakeDateTime(date(2026, 3, 9))))

    def test_queue_alert_state_resets_only_after_recovery_to_10_or_more(self):
        self.assertEqual(_reset_queue_alert_state(0, True, True), (True, True))
        self.assertEqual(_reset_queue_alert_state(5, True, True), (True, True))
        self.assertEqual(_reset_queue_alert_state(9, True, True), (True, True))
        self.assertEqual(_reset_queue_alert_state(10, True, True), (False, False))

    def test_daily_limit_blocks_additional_alerts(self):
        class FakeDateTime:
            def __init__(self, year, month, day, hour=12, minute=0):
                self.year = year
                self.month = month
                self.day = day
                self.hour = hour
                self.minute = minute

            def date(self):
                return date(self.year, self.month, self.day)

        state = _initial_alert_runtime_state()
        state["sent_today"] = 150
        today = state["day"]
        allowed = _should_send_alert(
            state,
            "queue_low",
            FakeDateTime(today.year, today.month, today.day),
            now_ts=1000.0,
        )
        self.assertFalse(allowed)

    def test_job_fail_alert_cooldown_suppresses_and_tracks_count(self):
        class FakeDateTime:
            def __init__(self, year, month, day, hour=12, minute=0):
                self.year = year
                self.month = month
                self.day = day
                self.hour = hour
                self.minute = minute

            def date(self):
                return date(self.year, self.month, self.day)

        state = _initial_alert_runtime_state()
        state["job_fail_cooldown_until"] = 500.0
        today = state["day"]
        allowed = _should_send_alert(
            state,
            "job_fail",
            FakeDateTime(today.year, today.month, today.day),
            now_ts=100.0,
        )
        self.assertFalse(allowed)
        self.assertEqual(state["job_fail_suppressed_count"], 1)

    def test_job_fail_body_includes_suppressed_count_note(self):
        state = _initial_alert_runtime_state()
        state["job_fail_suppressed_count"] = 3
        body = _build_job_fail_alert_body("Failure details", state)
        self.assertIn("Suppressed 3 additional job start-failure email(s) during cooldown.", body)
        self.assertEqual(state["job_fail_suppressed_count"], 0)


class MonitorHostSourceTests(unittest.TestCase):
    def test_load_hosts_from_file_supports_comments_and_commas(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            hosts_file = Path(tmpdir) / "hosts.txt"
            hosts_file.write_text(
                "eci1, eci2\n"
                "eci2 eci3\n"
                "  # comment line\n"
                "eci4 # inline comment\n"
            )

            hosts = load_hosts_from_file(hosts_file)

        self.assertEqual(hosts, ["eci1", "eci2", "eci3", "eci4"])

    def test_monitor_loop_reloads_hosts_file_and_stops_launching_removed_hosts(self):
        class FakeStopEvent:
            def __init__(self, loops):
                self.loops = loops

            def is_set(self):
                return self.loops <= 0

            def wait(self, _timeout):
                self.loops -= 1
                return self.is_set()

        with tempfile.TemporaryDirectory() as tmpdir:
            hosts_file = Path(tmpdir) / "hosts.txt"
            hosts_file.write_text("eci1\neci2\n")
            status_calls = []
            launched_hosts = []
            dequeue_calls = []

            def fake_status_all(hosts):
                status_calls.append(list(hosts))
                if len(status_calls) == 1:
                    hosts_file.write_text("eci1\n")
                    return [
                        {"host": "eci1", "reachable": True, "pid": None},
                        {"host": "eci2", "reachable": True, "pid": None},
                    ]
                return [{"host": "eci1", "reachable": True, "pid": None}]

            def fake_dequeue_for_host(host, queue_host_map=None):
                dequeue_calls.append(host)
                if host == "eci2" and dequeue_calls.count("eci2") == 1:
                    return {"cmd": "echo run-once", "priority": 0, "hosts": None, "preempt": False}
                return None

            def fake_launch_job_on_host(host, _job_item, running_jobs):
                running_jobs[host] = {"cmd": "echo run-once"}
                launched_hosts.append(host)
                # The real _launch_job_on_host returns a dict; monitor_loop reads
                # `.get("launched")` on it. Returning a bare True (as this fake
                # used to) crashed the loop with AttributeError mid-iteration,
                # which is why this test was flaking.
                return {"launched": True}

            with patch("awsqueueengine.host.monitor.load_running_jobs", return_value={}), patch(
                "awsqueueengine.host.monitor.parse_email_recipients", return_value=[]
            ), patch("awsqueueengine.host.monitor._load_last_daily_summary_date", return_value=date(2026, 3, 10)), patch(
                "awsqueueengine.host.monitor.status_all", side_effect=fake_status_all
            ), patch(
                "awsqueueengine.host.monitor.dequeue_for_host", side_effect=fake_dequeue_for_host
            ), patch(
                "awsqueueengine.host.monitor._launch_job_on_host", side_effect=fake_launch_job_on_host
            ), patch(
                "awsqueueengine.host.monitor._prune_running_jobs_for_status", return_value=(False, [], [])
            ), patch(
                "awsqueueengine.host.monitor._select_preempt_target", return_value=(None, None, None)
            ), patch(
                "awsqueueengine.host.monitor.load_queue", return_value=[]
            ), patch(
                "awsqueueengine.host.monitor.save_queue"
            ), patch(
                "awsqueueengine.host.monitor.save_running_jobs"
            ), patch(
                "awsqueueengine.host.monitor.append_completed_records"
            ), patch(
                "awsqueueengine.host.monitor._send_alert_email_with_limits", return_value=True
            ), patch(
                "awsqueueengine.host.monitor._save_last_daily_summary_date"
            ):
                monitor_loop(
                    ["eci1", "eci2"],
                    poll_interval=0,
                    stop_event=FakeStopEvent(2),
                    hosts_file=hosts_file,
                )

        self.assertEqual(status_calls, [["eci1", "eci2"], ["eci1"]])
        self.assertEqual(launched_hosts, ["eci2"])
        self.assertEqual(dequeue_calls.count("eci2"), 1)


if __name__ == "__main__":
    unittest.main()
