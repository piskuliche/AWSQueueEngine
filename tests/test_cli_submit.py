import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
QUEUE_FILE_NAME = ".aws_slurm_like_queue.json"
RUNNING_FILE_NAME = ".aws_slurm_like_running.json"


class CliSubmitTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.home_path = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _run_cli(self, *args):
        env = os.environ.copy()
        env["HOME"] = str(self.home_path)
        src_path = str(REPO_ROOT / "src")
        existing_pythonpath = env.get("PYTHONPATH")
        if existing_pythonpath:
            env["PYTHONPATH"] = src_path + os.pathsep + existing_pythonpath
        else:
            env["PYTHONPATH"] = src_path

        cmd = [sys.executable, "-m", "awsqueueengine.cli", *args]
        return subprocess.run(cmd, cwd=str(REPO_ROOT), env=env, capture_output=True, text=True)

    def _read_queue(self):
        queue_file = self.home_path / QUEUE_FILE_NAME
        if not queue_file.exists():
            return []
        return json.loads(queue_file.read_text())

    def _write_running(self, payload):
        running_file = self.home_path / RUNNING_FILE_NAME
        running_file.write_text(json.dumps(payload, indent=2))

    def test_submit_with_hosts_persists_allowlist(self):
        res = self._run_cli("submit", "--hosts", "eci16", "--hosts", "eci18", "echo", "hello")

        self.assertEqual(res.returncode, 0)
        items = self._read_queue()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["cmd"], "echo hello")
        self.assertEqual(items[0]["hosts"], ["eci16", "eci18"])
        self.assertEqual(items[0]["priority"], 0)

    def test_submit_rejects_unknown_hosts(self):
        res = self._run_cli("submit", "--hosts", "typo-host", "echo", "hello")

        self.assertEqual(res.returncode, 1)
        self.assertIn("Invalid host(s): typo-host", res.stdout)
        self.assertEqual(self._read_queue(), [])

    def test_submit_priority_argument_sets_integer_priority(self):
        res = self._run_cli("submit", "--priority", "42", "echo", "hello")

        self.assertEqual(res.returncode, 0)
        items = self._read_queue()
        self.assertEqual(items[0]["priority"], 42)

    def test_submit_high_priority_alias_maps_to_100(self):
        res = self._run_cli("submit", "--high-priority", "echo", "hello")

        self.assertEqual(res.returncode, 0)
        items = self._read_queue()
        self.assertEqual(items[0]["priority"], 100)

    def test_submit_defaults_priority_to_zero(self):
        res = self._run_cli("submit", "echo", "hello")

        self.assertEqual(res.returncode, 0)
        items = self._read_queue()
        self.assertEqual(items[0]["priority"], 0)

    def test_submit_preempt_flag_persists(self):
        res = self._run_cli("submit", "--preempt", "echo", "hello")

        self.assertEqual(res.returncode, 0)
        items = self._read_queue()
        self.assertTrue(items[0]["preempt"])

    def test_qdel_removes_single_job_by_index(self):
        self._run_cli("submit", "echo", "one")
        self._run_cli("submit", "echo", "two")
        self._run_cli("submit", "echo", "three")

        res = self._run_cli("qdel", "2")

        self.assertEqual(res.returncode, 0)
        self.assertIn("Removed 1 job(s).", res.stdout)
        items = self._read_queue()
        self.assertEqual([item["cmd"] for item in items], ["echo one", "echo three"])

    def test_qdel_removes_multiple_jobs_atomically(self):
        self._run_cli("submit", "echo", "one")
        self._run_cli("submit", "echo", "two")
        self._run_cli("submit", "echo", "three")
        self._run_cli("submit", "echo", "four")

        res = self._run_cli("qdel", "1", "3")

        self.assertEqual(res.returncode, 0)
        self.assertIn("Removed 2 job(s).", res.stdout)
        items = self._read_queue()
        self.assertEqual([item["cmd"] for item in items], ["echo two", "echo four"])

    def test_qdel_rejects_invalid_index_without_changes(self):
        self._run_cli("submit", "echo", "one")
        self._run_cli("submit", "echo", "two")

        res = self._run_cli("qdel", "3")

        self.assertEqual(res.returncode, 1)
        self.assertIn("Invalid queue index(es): 3", res.stdout)
        items = self._read_queue()
        self.assertEqual([item["cmd"] for item in items], ["echo one", "echo two"])

    def test_qstat_lists_running_jobs(self):
        self._write_running(
            {
                "eci5": {
                    "cmd": "bash run.sh --epochs 5",
                    "payload_remote_path": "/home/ubuntu/1scratch/myjob-abc123",
                    "priority": 42,
                    "preempt": False,
                    "hosts": ["eci5"],
                    "started_at": 1,
                }
            }
        )

        res = self._run_cli("qstat")

        self.assertEqual(res.returncode, 0)
        self.assertIn("HOST", res.stdout)
        self.assertIn("DUR", res.stdout)
        self.assertIn("eci5", res.stdout)
        self.assertIn("bash run.sh --epochs 5", res.stdout)
        self.assertRegex(res.stdout, r"\d{2}:\d{2}:\d{2}")


if __name__ == "__main__":
    unittest.main()
