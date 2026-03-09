import unittest
from datetime import date

from awsqueueengine.monitor import (
    _build_job_fail_alert_body,
    _initial_alert_runtime_state,
    _prune_running_jobs_for_status,
    _reset_queue_alert_state,
    _should_send_alert,
    _should_send_daily_summary,
    _should_send_empty_queue_alert,
    _should_send_low_queue_alert,
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

        changed = _prune_running_jobs_for_status(running_jobs, status_rows)

        self.assertFalse(changed)
        self.assertEqual(set(running_jobs), {"eci5", "eci6"})

    def test_reachable_idle_host_is_pruned(self):
        running_jobs = {
            "eci5": {"cmd": "run-a"},
            "eci6": {"cmd": "run-b"},
        }
        status_rows = [
            {"host": "eci5", "reachable": True, "pid": None},
            {"host": "eci6", "reachable": True, "pid": "123"},
        ]

        changed = _prune_running_jobs_for_status(running_jobs, status_rows)

        self.assertTrue(changed)
        self.assertEqual(set(running_jobs), {"eci6"})


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
        allowed = _should_send_alert(state, "queue_low", FakeDateTime(2026, 3, 9), now_ts=1000.0)
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
        allowed = _should_send_alert(state, "job_fail", FakeDateTime(2026, 3, 9), now_ts=100.0)
        self.assertFalse(allowed)
        self.assertEqual(state["job_fail_suppressed_count"], 1)

    def test_job_fail_body_includes_suppressed_count_note(self):
        state = _initial_alert_runtime_state()
        state["job_fail_suppressed_count"] = 3
        body = _build_job_fail_alert_body("Failure details", state)
        self.assertIn("Suppressed 3 additional job start-failure email(s) during cooldown.", body)
        self.assertEqual(state["job_fail_suppressed_count"], 0)


if __name__ == "__main__":
    unittest.main()
