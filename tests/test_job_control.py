import unittest
from unittest.mock import patch

from awsqueueengine.job_control import kill_managed_on_host, submit_to_host
from awsqueueengine.config import REMOTE_LOG_DIR


class KillManagedOnHostTests(unittest.TestCase):
    def test_kill_prefers_pidfiles_under_remote_log_dir(self):
        captured = {}

        def fake_ssh_run(host, cmd):
            captured["host"] = host
            captured["cmd"] = cmd
            return 0, "", ""

        result = kill_managed_on_host("eci5", ssh_run=fake_ssh_run, grace_seconds=7)

        self.assertEqual(result["host"], "eci5")
        self.assertEqual(result["rc"], 0)
        self.assertIn(f"ls -1 {REMOTE_LOG_DIR}/*.pid", captured["cmd"])
        self.assertIn('ps -p "$pid" -o pid=', captured["cmd"])
        self.assertIn('rm -f "$pidfile"', captured["cmd"])
        self.assertIn("sleep 7", captured["cmd"])
        self.assertIn("exit 0", captured["cmd"])

    def test_kill_keeps_pgrep_fallback_for_untracked_processes(self):
        def fake_ssh_run(_host, cmd):
            self.assertIn("pgrep -f '[M]ANAGER_TAG='", cmd)
            self.assertIn("pgrep -P", cmd)
            return 0, "", ""

        result = kill_managed_on_host("eci6", ssh_run=fake_ssh_run)

        self.assertEqual(result["rc"], 0)

    def test_kill_uses_self_safe_pkill_patterns(self):
        captured = {}

        def fake_ssh_run(_host, cmd):
            captured["cmd"] = cmd
            return 0, "", ""

        result = kill_managed_on_host("eci7", ssh_run=fake_ssh_run)

        self.assertEqual(result["rc"], 0)
        self.assertIn("pkill -f '[p]memd.cuda'", captured["cmd"])
        self.assertIn("pkill -f '[p]memd.cuda.MPI'", captured["cmd"])


class SubmitToHostS3PayloadTests(unittest.TestCase):
    def test_s3_payload_is_downloaded_before_launch(self):
        commands = []

        def fake_ssh_run(_host, cmd, timeout=60, capture_output=True):
            commands.append(cmd)
            if "aws s3 cp" in cmd:
                return 0, "", ""
            if "echo $!" in cmd:
                return 0, "", ""
            if cmd.startswith("cat "):
                return 0, "1234", ""
            if "ps -p 1234" in cmd:
                return 0, "1234", ""
            return 0, "", ""

        with patch("awsqueueengine.job_control.choose_scratch_on_host", return_value=("/scratch", 1000)), patch(
            "awsqueueengine.job_control.ssh_run", side_effect=fake_ssh_run
        ):
            result = submit_to_host(
                "eci5",
                "bash run.sh",
                payload_s3_uri="s3://bucket/payload.tar.gz",
                payload_size_bytes=500,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["payload"], "/scratch/payload-" + result["tag"])
        download_cmd = commands[0]
        self.assertIn("bash -lc", download_cmd)
        self.assertIn("set -euo pipefail", download_cmd)
        self.assertIn("aws s3 cp", download_cmd)
        self.assertIn("s3://bucket/payload.tar.gz", download_cmd)
        self.assertIn("tar xzf", download_cmd)
        self.assertIn("PAYLOAD_DIR=/scratch/payload-", commands[1])

    def test_s3_download_failure_returns_payload_for_requeue_reuse(self):
        def fake_ssh_run(_host, cmd, timeout=60, capture_output=True):
            if "aws s3 cp" in cmd:
                return 1, "", "download failed"
            return 0, "", ""

        with patch("awsqueueengine.job_control.choose_scratch_on_host", return_value=("/scratch", 1000)), patch(
            "awsqueueengine.job_control.ssh_run", side_effect=fake_ssh_run
        ):
            result = submit_to_host(
                "eci5",
                "bash run.sh",
                payload_s3_uri="s3://bucket/payload.tar.gz",
                payload_size_bytes=500,
            )

        self.assertFalse(result["ok"])
        self.assertIn("s3 payload download failed", result["err"])
        self.assertEqual(result["payload"], "/scratch/payload-" + result["tag"])


if __name__ == "__main__":
    unittest.main()
