import unittest
from datetime import date
from pathlib import Path
import tempfile
from unittest.mock import patch

from awsqueueengine.monitor import (
    _build_job_fail_alert_body,
    _build_completed_job_record,
    _initial_alert_runtime_state,
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

        changed, completed_records = _prune_running_jobs_for_status(running_jobs, status_rows)

        self.assertFalse(changed)
        self.assertEqual(completed_records, [])
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

        with patch("awsqueueengine.monitor.time.time", return_value=160.0):
            changed, completed_records = _prune_running_jobs_for_status(running_jobs, status_rows)

        self.assertTrue(changed)
        self.assertEqual(set(running_jobs), {"eci6"})
        self.assertEqual(len(completed_records), 1)
        self.assertEqual(completed_records[0]["host"], "eci5")
        self.assertEqual(completed_records[0]["dur"], "00:01:00")
        self.assertEqual(completed_records[0]["duration_seconds"], 60)
        self.assertEqual(completed_records[0]["cmd"], "run-a")

    def test_missing_host_in_status_keeps_running_metadata(self):
        running_jobs = {"eci9": {"cmd": "run-z"}}
        status_rows = [{"host": "eci8", "reachable": True, "pid": None}]

        changed, completed_records = _prune_running_jobs_for_status(running_jobs, status_rows)

        self.assertFalse(changed)
        self.assertEqual(completed_records, [])
        self.assertEqual(set(running_jobs), {"eci9"})

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

            def fake_dequeue_for_host(host):
                dequeue_calls.append(host)
                if host == "eci2" and dequeue_calls.count("eci2") == 1:
                    return {"cmd": "echo run-once", "priority": 0, "hosts": None, "preempt": False}
                return None

            def fake_launch_job_on_host(host, _job_item, running_jobs):
                running_jobs[host] = {"cmd": "echo run-once"}
                launched_hosts.append(host)
                return True

            with patch("awsqueueengine.monitor.load_running_jobs", return_value={}), patch(
                "awsqueueengine.monitor.parse_email_recipients", return_value=[]
            ), patch("awsqueueengine.monitor._load_last_daily_summary_date", return_value=date(2026, 3, 10)), patch(
                "awsqueueengine.monitor.status_all", side_effect=fake_status_all
            ), patch(
                "awsqueueengine.monitor.dequeue_for_host", side_effect=fake_dequeue_for_host
            ), patch(
                "awsqueueengine.monitor._launch_job_on_host", side_effect=fake_launch_job_on_host
            ), patch(
                "awsqueueengine.monitor._prune_running_jobs_for_status", return_value=(False, [])
            ), patch(
                "awsqueueengine.monitor._select_preempt_target", return_value=(None, None, None)
            ), patch(
                "awsqueueengine.monitor.load_queue", return_value=[]
            ), patch(
                "awsqueueengine.monitor.save_queue"
            ), patch(
                "awsqueueengine.monitor.save_running_jobs"
            ), patch(
                "awsqueueengine.monitor.append_completed_records"
            ), patch(
                "awsqueueengine.monitor._send_alert_email_with_limits", return_value=True
            ), patch(
                "awsqueueengine.monitor._save_last_daily_summary_date"
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
