import unittest

from awsqueueengine.job_control import kill_managed_on_host
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


if __name__ == "__main__":
    unittest.main()
