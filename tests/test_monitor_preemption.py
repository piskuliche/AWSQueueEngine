"""The preemption path claims its target instead of popping a snapshot index.

`monitor_loop` chooses a preemption target from a queue snapshot read some time
earlier, then sends email and talks to hosts over SSH before acting on it. It
used to `queue_items.pop(idx)` and write that snapshot back, which erased
anything enqueued in the meantime. It now re-claims the job by identity under
the state lock, and does nothing if the job is already gone.
"""
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from awsqueueengine.host.monitor import monitor_loop
from awsqueueengine.shared import queue


class StopAfter:
    """A stop_event that lets `monitor_loop` run a fixed number of cycles."""

    def __init__(self, loops):
        self.loops = loops

    def is_set(self):
        return self.loops <= 0

    def wait(self, _timeout):
        self.loops -= 1
        return self.is_set()


class PreemptionClaimTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.original_queue_file = queue.QUEUE_FILE
        queue.QUEUE_FILE = Path(self.tmpdir.name) / "queue.json"

    def tearDown(self):
        queue.QUEUE_FILE = self.original_queue_file
        self.tmpdir.cleanup()

    def _run_one_cycle(self, preempt_item, victim_host="eci1", kill_rc=0):
        """One monitor cycle with everything but the preemption path stubbed out."""
        kill_calls = []
        launched = []

        def fake_kill(host):
            kill_calls.append(host)
            return {"rc": kill_rc, "out": "", "err": ""}

        def fake_launch(host, job_item, running_jobs):
            launched.append((host, job_item.get("job_id")))
            return {"launched": True}

        status_rows = [{"host": victim_host, "reachable": True, "pid": "123"}]
        with patch("awsqueueengine.host.monitor.load_running_jobs", return_value={}), patch(
            "awsqueueengine.host.monitor.parse_email_recipients", return_value=[]
        ), patch(
            "awsqueueengine.host.monitor._load_last_daily_summary_date",
            return_value=date(2026, 3, 10),
        ), patch(
            "awsqueueengine.host.monitor.status_all", return_value=status_rows
        ), patch(
            "awsqueueengine.host.monitor._prune_running_jobs_for_status",
            return_value=(False, [], []),
        ), patch(
            "awsqueueengine.host.monitor._select_preempt_target",
            return_value=(0, preempt_item, victim_host),
        ), patch(
            "awsqueueengine.host.monitor.kill_managed_on_host", side_effect=fake_kill
        ), patch(
            "awsqueueengine.host.monitor._launch_job_on_host", side_effect=fake_launch
        ), patch(
            "awsqueueengine.host.monitor.save_running_jobs"
        ), patch(
            "awsqueueengine.host.monitor._send_alert_email_with_limits", return_value=True
        ):
            monitor_loop([victim_host], poll_interval=0, stop_event=StopAfter(1))
        return kill_calls, launched

    def test_claims_the_target_and_leaves_concurrent_submissions_alone(self):
        target = {"cmd": "urgent", "job_id": "job-preempt", "preempt": True, "priority": 100}
        queue.enqueue_item(target)
        # Submitted after the snapshot the monitor would have been holding.
        queue.enqueue_item({"cmd": "submitted-mid-cycle", "job_id": "job-new"})

        kill_calls, launched = self._run_one_cycle(queue.normalize_job_item(target))

        self.assertEqual(kill_calls, ["eci1"])
        self.assertEqual(launched, [("eci1", "job-preempt")])
        # The whole bug in one assertion: the mid-cycle submission survives.
        self.assertEqual([item["job_id"] for item in queue.load_queue()], ["job-new"])

    def test_skips_preemption_when_the_target_is_already_gone(self):
        # Chosen from the snapshot, then qdel'd (or dispatched) before we act.
        target = queue.normalize_job_item({"cmd": "urgent", "job_id": "job-preempt"})
        queue.enqueue_item({"cmd": "unrelated", "job_id": "job-other"})

        kill_calls, launched = self._run_one_cycle(target)

        self.assertEqual(kill_calls, [], "killed a host for a job that no longer exists")
        self.assertEqual(launched, [])
        self.assertEqual([item["job_id"] for item in queue.load_queue()], ["job-other"])

    def test_failed_kill_puts_the_claimed_job_back_at_the_front(self):
        target = {"cmd": "urgent", "job_id": "job-preempt", "preempt": True, "priority": 100}
        queue.enqueue_item({"cmd": "waiting", "job_id": "job-other"})
        queue.enqueue_item(target)

        kill_calls, launched = self._run_one_cycle(
            queue.normalize_job_item(target), kill_rc=1
        )

        self.assertEqual(kill_calls, ["eci1"])
        self.assertEqual(launched, [])
        self.assertEqual(
            [item["job_id"] for item in queue.load_queue()], ["job-preempt", "job-other"]
        )


if __name__ == "__main__":
    unittest.main()
