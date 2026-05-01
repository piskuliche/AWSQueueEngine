import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
QUEUE_FILE_NAME = ".aws_slurm_like_queue.json"
RUNNING_FILE_NAME = ".aws_slurm_like_running.json"


class CliSubmitTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.home_path = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _run_cli(self, *args, env_extra=None):
        env = os.environ.copy()
        for key in (
            "AWSQUEUEENGINE_MAILTRAP_TOKEN",
            "AWSQUEUEENGINE_MAILTRAP_SENDER_EMAIL",
            "AWSQUEUEENGINE_MAILTRAP_SENDER_NAME",
            "AWSQUEUEENGINE_MAILTRAP_CATEGORY",
            "AWSQUEUEENGINE_ALERT_TO",
            "AWSQUEUEENGINE_ALERT_DAILY_EMAIL_LIMIT",
            "AWSQUEUEENGINE_JOB_FAIL_ALERT_COOLDOWN_SECONDS",
            "AWSQUEUEENGINE_HOSTS_FILE",
            "AWSQUEUEENGINE_S3_BUCKET",
            "AWSQUEUEENGINE_S3_PREFIX",
            "AWSQUEUEENGINE_HOST_SET_FAST",
            "AWSQUEUEENGINE_HOSTS_FILE_FAST",
            "AWSQUEUEENGINE_QUEUES",
            "AWSQUEUEENGINE_QUEUES_FILE",
        ):
            env.pop(key, None)
        if env_extra:
            env.update(env_extra)
        env["HOME"] = str(self.home_path)
        src_path = str(REPO_ROOT / "src")
        existing_pythonpath = env.get("PYTHONPATH")
        if existing_pythonpath:
            env["PYTHONPATH"] = src_path + os.pathsep + existing_pythonpath
        else:
            env["PYTHONPATH"] = src_path

        cmd = [sys.executable, "-m", "awsqueueengine.cli", *args]
        return subprocess.run(cmd, cwd=str(REPO_ROOT), env=env, capture_output=True, text=True)

    def _run_cli_with_path_prefix(self, path_prefix, *args):
        env = os.environ.copy()
        for key in (
            "AWSQUEUEENGINE_MAILTRAP_TOKEN",
            "AWSQUEUEENGINE_MAILTRAP_SENDER_EMAIL",
            "AWSQUEUEENGINE_MAILTRAP_SENDER_NAME",
            "AWSQUEUEENGINE_MAILTRAP_CATEGORY",
            "AWSQUEUEENGINE_ALERT_TO",
            "AWSQUEUEENGINE_ALERT_DAILY_EMAIL_LIMIT",
            "AWSQUEUEENGINE_JOB_FAIL_ALERT_COOLDOWN_SECONDS",
            "AWSQUEUEENGINE_HOSTS_FILE",
            "AWSQUEUEENGINE_S3_BUCKET",
            "AWSQUEUEENGINE_S3_PREFIX",
            "AWSQUEUEENGINE_HOST_SET_FAST",
            "AWSQUEUEENGINE_HOSTS_FILE_FAST",
            "AWSQUEUEENGINE_QUEUES",
            "AWSQUEUEENGINE_QUEUES_FILE",
        ):
            env.pop(key, None)
        env["HOME"] = str(self.home_path)
        src_path = str(REPO_ROOT / "src")
        existing_pythonpath = env.get("PYTHONPATH")
        if existing_pythonpath:
            env["PYTHONPATH"] = src_path + os.pathsep + existing_pythonpath
        else:
            env["PYTHONPATH"] = src_path
        env["PATH"] = str(path_prefix) + os.pathsep + env["PATH"]

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

    def _make_fake_ssh(self, exit_code=0):
        bin_dir = self.home_path / "bin"
        bin_dir.mkdir(exist_ok=True)
        ssh_path = bin_dir / "ssh"
        ssh_path.write_text(f"#!/bin/sh\nexit {exit_code}\n")
        ssh_path.chmod(0o755)
        return bin_dir

    def _make_fake_ssh_capture(self, capture_path, exit_code=0, stdout="remote ok"):
        bin_dir = self.home_path / "bin"
        bin_dir.mkdir(exist_ok=True)
        ssh_path = bin_dir / "ssh"
        ssh_path.write_text(
            "#!/bin/sh\n"
            f"printf '%s\\n' \"$@\" > {str(capture_path)!r}\n"
            f"printf '%s\\n' {stdout!r}\n"
            f"exit {exit_code}\n"
        )
        ssh_path.chmod(0o755)
        return bin_dir

    def _write_hosts_file(self, content):
        hosts_file = self.home_path / "hosts.txt"
        hosts_file.write_text(content)
        return hosts_file

    def test_submit_with_hosts_persists_allowlist(self):
        res = self._run_cli("submit", "--hosts", "eci16", "--hosts", "eci18", "echo", "hello")

        self.assertEqual(res.returncode, 0)
        items = self._read_queue()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["cmd"], "echo hello")
        self.assertEqual(items[0]["queue"], "default")
        self.assertEqual(items[0]["hosts"], ["eci16", "eci18"])
        self.assertEqual(items[0]["priority"], 0)

    def test_submit_rejects_unknown_hosts(self):
        res = self._run_cli("submit", "--hosts", "typo-host", "echo", "hello")

        self.assertEqual(res.returncode, 1)
        self.assertIn("Invalid host(s): typo-host", res.stdout)
        self.assertEqual(self._read_queue(), [])

    def test_submit_uses_hosts_file_for_host_validation(self):
        hosts_file = self._write_hosts_file("eci16\neci18\n")

        res = self._run_cli(
            "submit",
            "--hosts-file",
            str(hosts_file),
            "--hosts",
            "eci16",
            "echo",
            "hello",
        )

        self.assertEqual(res.returncode, 0)
        items = self._read_queue()
        self.assertEqual(items[0]["hosts"], ["eci16"])

    def test_submit_rejects_host_not_present_in_hosts_file(self):
        hosts_file = self._write_hosts_file("eci16\neci18\n")

        res = self._run_cli(
            "submit",
            "--hosts-file",
            str(hosts_file),
            "--hosts",
            "eci17",
            "echo",
            "hello",
        )

        self.assertEqual(res.returncode, 1)
        self.assertIn("Invalid host(s): eci17", res.stdout)
        self.assertIn("Valid hosts: eci16, eci18", res.stdout)
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

    def test_submit_queue_persists_queue_name_without_host_allowlist(self):
        res = self._run_cli(
            "submit",
            "--queue",
            "fast",
            "echo",
            "hello",
            env_extra={"AWSQUEUEENGINE_QUEUES": "default=eci1;fast=eci16,eci18"},
        )

        self.assertEqual(res.returncode, 0)
        items = self._read_queue()
        self.assertEqual(items[0]["queue"], "fast")
        self.assertIsNone(items[0]["hosts"])

    def test_submit_host_set_alias_uses_legacy_host_set_as_queue(self):
        res = self._run_cli(
            "submit",
            "--host-set",
            "fast",
            "echo",
            "hello",
            env_extra={"AWSQUEUEENGINE_HOST_SET_FAST": "eci16, eci18"},
        )

        self.assertEqual(res.returncode, 0)
        items = self._read_queue()
        self.assertEqual(items[0]["queue"], "fast")
        self.assertIsNone(items[0]["hosts"])

    def test_submit_rejects_unknown_host_set(self):
        res = self._run_cli("submit", "--host-set", "fast", "echo", "hello")

        self.assertEqual(res.returncode, 1)
        self.assertIn("Unknown queue 'fast'", res.stdout)
        self.assertEqual(self._read_queue(), [])

    def test_submit_internal_s3_payload_fields_persist(self):
        res = self._run_cli(
            "submit",
            "--payload-s3-uri",
            "s3://bucket/payload.tar.gz",
            "--payload-size-bytes",
            "55",
            "echo",
            "hello",
        )

        self.assertEqual(res.returncode, 0)
        items = self._read_queue()
        self.assertEqual(len(items), 1)
        self.assertIsNone(items[0]["payload"])
        self.assertEqual(items[0]["payload_s3_uri"], "s3://bucket/payload.tar.gz")
        self.assertEqual(items[0]["payload_size_bytes"], 55)

    def test_submit_rejects_missing_payload_path(self):
        missing = self.home_path / "does_not_exist"

        res = self._run_cli("submit", "--payload", str(missing), "echo", "hello")

        self.assertEqual(res.returncode, 1)
        self.assertIn(f"Payload not found on local filesystem: {missing}", res.stdout)
        self.assertEqual(self._read_queue(), [])

    def test_submit_accepts_existing_payload_path(self):
        payload_dir = self.home_path / "payload"
        payload_dir.mkdir()
        (payload_dir / "run.sh").write_text("echo hi\n")

        res = self._run_cli("submit", "--payload", str(payload_dir), "echo", "hello")

        self.assertEqual(res.returncode, 0)
        items = self._read_queue()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["payload"], str(payload_dir))

    def test_remote_submit_rejects_missing_payload_path_before_ssh(self):
        capture_path = self.home_path / "ssh_args.txt"
        fake_ssh_dir = self._make_fake_ssh_capture(capture_path)
        missing = self.home_path / "does_not_exist"

        res = self._run_cli_with_path_prefix(
            fake_ssh_dir,
            "submit",
            "--queue-host",
            "queuebox",
            "--payload",
            str(missing),
            "echo",
            "hello",
        )

        self.assertEqual(res.returncode, 1)
        self.assertIn(f"Payload not found on local filesystem: {missing}", res.stdout)
        self.assertFalse(capture_path.exists())

    def test_remote_submit_without_payload_forwards_over_ssh_and_does_not_write_local_queue(self):
        capture_path = self.home_path / "ssh_args.txt"
        fake_ssh_dir = self._make_fake_ssh_capture(capture_path)

        res = self._run_cli_with_path_prefix(
            fake_ssh_dir,
            "submit",
            "--queue-host",
            "queuebox",
            "--hosts",
            "eci17",
            "--priority",
            "5",
            "echo",
            "hello world",
        )

        self.assertEqual(res.returncode, 0)
        self.assertIn("remote ok", res.stdout)
        self.assertEqual(self._read_queue(), [])
        captured = capture_path.read_text().splitlines()
        self.assertEqual(captured[0], "queuebox")
        self.assertIn("awsqueueengine submit", captured[1])
        self.assertIn("--hosts eci17", captured[1])
        self.assertIn("--priority 5", captured[1])
        self.assertIn("'echo hello world'", captured[1])

    def test_remote_submit_rejects_host_set_with_hosts(self):
        res = self._run_cli(
            "submit",
            "--queue-host",
            "queuebox",
            "--host-set",
            "fast",
            "--hosts",
            "eci17",
            "echo",
            "hello",
        )

        self.assertEqual(res.returncode, 1)
        self.assertIn("--host-set and --hosts cannot be used together", res.stdout)
        self.assertEqual(self._read_queue(), [])

    def test_remote_submit_with_host_set_forwards_name_to_queue_host(self):
        capture_path = self.home_path / "ssh_args.txt"
        fake_ssh_dir = self._make_fake_ssh_capture(capture_path)

        res = self._run_cli_with_path_prefix(
            fake_ssh_dir,
            "submit",
            "--queue-host",
            "queuebox",
            "--host-set",
            "fast",
            "--priority",
            "5",
            "echo",
            "hello world",
        )

        self.assertEqual(res.returncode, 0)
        self.assertIn("remote ok", res.stdout)
        self.assertEqual(self._read_queue(), [])
        captured = capture_path.read_text().splitlines()
        self.assertEqual(captured[0], "queuebox")
        self.assertIn("awsqueueengine submit", captured[1])
        self.assertIn("--queue fast", captured[1])
        self.assertIn("--priority 5", captured[1])
        self.assertIn("'echo hello world'", captured[1])

    def test_remote_submit_rejects_local_hosts_file(self):
        hosts_file = self._write_hosts_file("eci1\n")

        res = self._run_cli("submit", "--queue-host", "queuebox", "--hosts-file", str(hosts_file), "echo", "hello")

        self.assertEqual(res.returncode, 1)
        self.assertIn("--hosts-file is not supported with --queue-host", res.stdout)
        self.assertEqual(self._read_queue(), [])

    def test_remote_submit_with_payload_uploads_s3_and_forwards_internal_fields(self):
        payload_dir = self.home_path / "payload"
        payload_dir.mkdir()
        (payload_dir / "run.sh").write_text("echo hi\n")

        class Args:
            queue_host = "queuebox"
            hosts_file = None
            payload = str(payload_dir)
            host_set = None
            hosts = ["eci17"]
            priority = None
            high_priority = True
            preempt = True

        captured = {}

        def fake_run_remote_submit(queue_host, remote_argv):
            captured["queue_host"] = queue_host
            captured["remote_argv"] = remote_argv

            class Result:
                returncode = 0
                stdout = "remote enqueued\n"
                stderr = ""

            return Result()

        from awsqueueengine import cli

        with patch("awsqueueengine.cli._upload_payload_archive_to_s3", return_value="s3://bucket/key.tar.gz") as upload, patch(
            "awsqueueengine.cli._run_remote_submit", side_effect=fake_run_remote_submit
        ):
            cli._handle_remote_submit(Args(), "bash run.sh")

        upload.assert_called_once()
        self.assertEqual(captured["queue_host"], "queuebox")
        self.assertIn("--payload-s3-uri", captured["remote_argv"])
        self.assertIn("s3://bucket/key.tar.gz", captured["remote_argv"])
        self.assertIn("--payload-size-bytes", captured["remote_argv"])
        self.assertIn("--high-priority", captured["remote_argv"])
        self.assertIn("--preempt", captured["remote_argv"])

    def test_requeue_running_requeues_even_when_kill_fails(self):
        fake_ssh_dir = self._make_fake_ssh(exit_code=1)
        self._write_running(
            {
                "eci5": {
                    "cmd": "bash run.sh",
                    "payload": "/tmp/local",
                    "payload_remote_path": "/remote/payload",
                    "priority": 7,
                    "hosts": ["eci1", "eci5"],
                    "preempt": False,
                    "started_at": 1,
                }
            }
        )

        res = self._run_cli_with_path_prefix(fake_ssh_dir, "requeue-running", "--hosts", "eci5")

        self.assertEqual(res.returncode, 0)
        self.assertIn("Requeued running job for eci5 at priority 100", res.stdout)
        self.assertIn("Kill error on eci5", res.stdout)
        items = self._read_queue()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["cmd"], "bash run.sh")
        self.assertEqual(items[0]["priority"], 100)
        self.assertEqual(items[0]["hosts"], ["eci5"])
        self.assertEqual(items[0]["payload_remote_path"], "/remote/payload")
        self.assertTrue(items[0]["resume_first"])
        self.assertEqual(items[0]["resume_host"], "eci5")

    def test_requeue_running_all_targets_all_tracked_running_hosts(self):
        fake_ssh_dir = self._make_fake_ssh(exit_code=0)
        self._write_running(
            {
                "eci5": {
                    "cmd": "echo one",
                    "payload_remote_path": "/remote/one",
                    "priority": 1,
                    "hosts": ["eci5"],
                    "started_at": 1,
                },
                "eci7": {
                    "cmd": "echo two",
                    "payload_remote_path": "/remote/two",
                    "priority": 2,
                    "hosts": ["eci7"],
                    "started_at": 2,
                },
            }
        )

        res = self._run_cli_with_path_prefix(fake_ssh_dir, "requeue-running", "--all")

        self.assertEqual(res.returncode, 0)
        self.assertIn("Requeued running job for eci5 at priority 100", res.stdout)
        self.assertIn("Requeued running job for eci7 at priority 100", res.stdout)
        items = self._read_queue()
        self.assertEqual(len(items), 2)
        by_host = {tuple(item["hosts"]): item for item in items}
        self.assertEqual(by_host[("eci5",)]["payload_remote_path"], "/remote/one")
        self.assertEqual(by_host[("eci7",)]["payload_remote_path"], "/remote/two")
        self.assertEqual(by_host[("eci5",)]["priority"], 100)
        self.assertEqual(by_host[("eci7",)]["priority"], 100)

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
        self.assertIn("QUEUE", res.stdout)
        self.assertIn("eci5", res.stdout)
        self.assertIn("bash run.sh --epochs 5", res.stdout)
        self.assertRegex(res.stdout, r"\d{2}:\d{2}:\d{2}")

    def test_test_email_connection_skips_when_not_configured(self):
        res = self._run_cli("--test-email-connection")

        self.assertEqual(res.returncode, 0)
        self.assertIn("Email test skipped", res.stdout)

    def test_list_does_not_print_startup_banner(self):
        res = self._run_cli("list")

        self.assertEqual(res.returncode, 0)
        self.assertNotIn("Starting the queue engine", res.stdout)
        self.assertIn("(queue empty)", res.stdout)


if __name__ == "__main__":
    unittest.main()
