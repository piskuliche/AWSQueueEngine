import contextlib
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from awsqueueengine.client import ledger as ledger_mod


REPO_ROOT = Path(__file__).resolve().parents[1]
# Phase 5 moved state files under ~/.awsqe/host/. The subprocess tests below
# point HOME at a tempdir, so the daemon writes/reads them at <tempdir>/.awsqe/host/.
QUEUE_FILE_REL = Path(".awsqe") / "host" / "queue.json"
RUNNING_FILE_REL = Path(".awsqe") / "host" / "running.json"
# The client's own tracked-job ledger, alongside its config.
LEDGER_FILE_REL = Path(".awsqe") / "client" / "jobs.json"


class CliSubmitTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.home_path = Path(self.tmpdir.name)
        # Only the subprocess helpers below override $HOME; the in-process
        # tests call client CLI handlers directly, and those now touch the
        # tracked-job ledger. Without this they would write to the developer's
        # real ~/.awsqe/client/jobs.json.
        self._original_ledger = ledger_mod.LEDGER_PATH
        ledger_mod.LEDGER_PATH = self.home_path / ".awsqe" / "client" / "jobs.json"

    def tearDown(self):
        ledger_mod.LEDGER_PATH = self._original_ledger
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
        queue_file = self.home_path / QUEUE_FILE_REL
        if not queue_file.exists():
            return []
        return json.loads(queue_file.read_text())

    def _write_running(self, payload):
        running_file = self.home_path / RUNNING_FILE_REL
        running_file.parent.mkdir(parents=True, exist_ok=True)
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

    def _make_fake_ssh_rpc(self, capture_path, stdin_path, result):
        """Fake `ssh <host> awsqe-host rpc` that captures argv + stdin and returns a JSON response."""
        bin_dir = self.home_path / "bin"
        bin_dir.mkdir(exist_ok=True)
        ssh_path = bin_dir / "ssh"
        response = json.dumps({"version": 1, "ok": True, "result": result})
        ssh_path.write_text(
            "#!/bin/sh\n"
            f"printf '%s\\n' \"$@\" > {str(capture_path)!r}\n"
            f"cat > {str(stdin_path)!r}\n"
            f"printf '%s' {response!r}\n"
            "exit 0\n"
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

    def test_submit_mps_flag_persists(self):
        res = self._run_cli("submit", "--mps", "echo", "hello")

        self.assertEqual(res.returncode, 0)
        items = self._read_queue()
        self.assertTrue(items[0]["mps"])

    def test_submit_defaults_mps_to_false(self):
        res = self._run_cli("submit", "echo", "hello")

        self.assertEqual(res.returncode, 0)
        items = self._read_queue()
        self.assertFalse(items[0]["mps"])

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
        stdin_path = self.home_path / "ssh_stdin.txt"
        fake_ssh_dir = self._make_fake_ssh_rpc(
            capture_path,
            stdin_path,
            result={"job_id": "JOB", "queue": "default", "hosts": ["eci17"]},
        )

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
        self.assertIn("Submitted ", res.stdout)
        self.assertEqual(self._read_queue(), [])
        captured = capture_path.read_text().splitlines()
        self.assertEqual(captured[0], "queuebox")
        self.assertEqual(captured[1], "awsqe-host")
        self.assertEqual(captured[2], "rpc")
        request = json.loads(stdin_path.read_text())
        self.assertEqual(request["version"], 1)
        self.assertEqual(request["method"], "enqueue")
        params = request["params"]
        self.assertEqual(params["cmd"], "echo hello world")
        self.assertEqual(params["hosts"], ["eci17"])
        self.assertEqual(params["priority"], 5)

    def _read_ledger(self):
        ledger_file = self.home_path / LEDGER_FILE_REL
        if not ledger_file.exists():
            return []
        return json.loads(ledger_file.read_text())["jobs"]

    def test_remote_submit_records_the_job_in_the_client_ledger(self):
        """End-to-end through a real process, so ~/.awsqe/client/jobs.json is resolved for real."""
        capture_path = self.home_path / "ssh_args.txt"
        stdin_path = self.home_path / "ssh_stdin.txt"
        fake_ssh_dir = self._make_fake_ssh_rpc(
            capture_path, stdin_path,
            result={"job_id": "JOB-42", "queue": "gpu", "hosts": ["eci17"]},
        )

        res = self._run_cli_with_path_prefix(
            fake_ssh_dir, "submit", "--queue-host", "queuebox", "--queue", "gpu",
            "echo", "hello world",
        )

        self.assertEqual(res.returncode, 0)
        tracked = self._read_ledger()
        self.assertEqual(len(tracked), 1)
        self.assertEqual(tracked[0]["job_id"], "JOB-42")
        self.assertEqual(tracked[0]["queue_host"], "queuebox")
        self.assertEqual(tracked[0]["queue"], "gpu")
        self.assertEqual(tracked[0]["cmd"], "echo hello world")
        self.assertEqual(tracked[0]["status"], "submitted")
        self.assertEqual(tracked[0]["payload"], "")

    def test_payloadless_submit_is_tracked_even_though_it_writes_no_run_info(self):
        """The gap this closes: without --payload there was previously no record at all."""
        capture_path = self.home_path / "ssh_args.txt"
        stdin_path = self.home_path / "ssh_stdin.txt"
        fake_ssh_dir = self._make_fake_ssh_rpc(
            capture_path, stdin_path, result={"job_id": "JOB-NP", "queue": "default"},
        )

        res = self._run_cli_with_path_prefix(
            fake_ssh_dir, "submit", "--queue-host", "queuebox", "sleep", "300",
        )

        self.assertEqual(res.returncode, 0)
        self.assertEqual([r["job_id"] for r in self._read_ledger()], ["JOB-NP"])

    def test_local_submit_does_not_touch_the_client_ledger(self):
        """A host-side submit isn't this client's job to track."""
        res = self._run_cli("submit", "echo", "hello")
        self.assertEqual(res.returncode, 0)
        self.assertEqual(self._read_ledger(), [])

    def test_remote_submit_forwards_mps_flag_over_ssh(self):
        capture_path = self.home_path / "ssh_args.txt"
        stdin_path = self.home_path / "ssh_stdin.txt"
        fake_ssh_dir = self._make_fake_ssh_rpc(
            capture_path,
            stdin_path,
            result={"job_id": "JOB", "queue": "default", "hosts": None},
        )

        res = self._run_cli_with_path_prefix(
            fake_ssh_dir,
            "submit",
            "--queue-host",
            "queuebox",
            "--mps",
            "echo",
            "hello",
        )

        self.assertEqual(res.returncode, 0)
        request = json.loads(stdin_path.read_text())
        self.assertEqual(request["method"], "enqueue")
        self.assertTrue(request["params"]["mps"])

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
        stdin_path = self.home_path / "ssh_stdin.txt"
        fake_ssh_dir = self._make_fake_ssh_rpc(
            capture_path,
            stdin_path,
            result={"job_id": "JOB", "queue": "fast", "hosts": None},
        )

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
        self.assertIn("Submitted ", res.stdout)
        self.assertEqual(self._read_queue(), [])
        captured = capture_path.read_text().splitlines()
        self.assertEqual(captured[0], "queuebox")
        self.assertEqual(captured[1], "awsqe-host")
        self.assertEqual(captured[2], "rpc")
        request = json.loads(stdin_path.read_text())
        self.assertEqual(request["method"], "enqueue")
        params = request["params"]
        self.assertEqual(params["queue"], "fast")
        self.assertEqual(params["priority"], 5)
        self.assertEqual(params["cmd"], "echo hello world")

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
            queue = None
            hosts = ["eci17"]
            priority = None
            high_priority = True
            preempt = True
            job_id = None

        captured = {}

        def fake_rpc_call(host, method, params, **kwargs):
            captured["queue_host"] = host
            captured["method"] = method
            captured["params"] = params
            return {"job_id": params.get("job_id"), "queue": params.get("queue"), "hosts": params.get("hosts")}

        from awsqueueengine.client import cli as client_cli

        with patch("awsqueueengine.client.cli.upload_payload_archive_to_s3", return_value="s3://bucket/key.tar.gz") as upload, patch(
            "awsqueueengine.client.cli.rpc_call", side_effect=fake_rpc_call
        ):
            client_cli.cmd_submit_remote(Args(), "bash run.sh")

        upload.assert_called_once()
        self.assertEqual(captured["queue_host"], "queuebox")
        self.assertEqual(captured["method"], "enqueue")
        params = captured["params"]
        self.assertEqual(params["payload_s3_uri"], "s3://bucket/key.tar.gz")
        self.assertIn("payload_size_bytes", params)
        self.assertTrue(params.get("high_priority"))
        self.assertTrue(params.get("preempt"))
        self.assertEqual(params["hosts"], ["eci17"])

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

    def test_requeue_running_mps_flag_forces_wrapper_on_requeued_job(self):
        fake_ssh_dir = self._make_fake_ssh(exit_code=0)
        self._write_running(
            {
                "eci5": {
                    "cmd": "bash run.sh",
                    "payload_remote_path": "/remote/payload",
                    "priority": 7,
                    "hosts": ["eci5"],
                    "started_at": 1,
                }
            }
        )

        res = self._run_cli_with_path_prefix(fake_ssh_dir, "requeue-running", "--hosts", "eci5", "--mps")

        self.assertEqual(res.returncode, 0)
        self.assertIn("with MPS enabled", res.stdout)
        items = self._read_queue()
        self.assertEqual(len(items), 1)
        self.assertTrue(items[0]["mps"])
        # In-place resume metadata is preserved (no re-stage of the payload).
        self.assertEqual(items[0]["payload_remote_path"], "/remote/payload")
        self.assertTrue(items[0]["resume_first"])

    def test_requeue_running_without_mps_preserves_existing_setting(self):
        fake_ssh_dir = self._make_fake_ssh(exit_code=0)
        self._write_running(
            {
                "eci5": {"cmd": "a", "mps": True, "hosts": ["eci5"], "started_at": 1},
                "eci7": {"cmd": "b", "mps": False, "hosts": ["eci7"], "started_at": 1},
            }
        )

        res = self._run_cli_with_path_prefix(fake_ssh_dir, "requeue-running", "--all")

        self.assertEqual(res.returncode, 0)
        by_cmd = {item["cmd"]: item for item in self._read_queue()}
        self.assertTrue(by_cmd["a"]["mps"])
        self.assertFalse(by_cmd["b"]["mps"])

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

    def _queued_job_ids(self):
        return [item["job_id"] for item in self._read_queue()]

    def test_qdel_removes_single_job_by_job_id(self):
        self._run_cli("submit", "echo", "one")
        self._run_cli("submit", "echo", "two")
        self._run_cli("submit", "echo", "three")
        job_ids = self._queued_job_ids()

        res = self._run_cli("qdel", job_ids[1])

        self.assertEqual(res.returncode, 0)
        self.assertIn("Removed 1 job(s).", res.stdout)
        items = self._read_queue()
        self.assertEqual([item["cmd"] for item in items], ["echo one", "echo three"])

    def test_qdel_removes_multiple_jobs_by_job_id(self):
        """The motivating case: two ids from one listing, no index renumbering."""
        self._run_cli("submit", "echo", "one")
        self._run_cli("submit", "echo", "two")
        self._run_cli("submit", "echo", "three")
        self._run_cli("submit", "echo", "four")
        job_ids = self._queued_job_ids()

        res = self._run_cli("qdel", job_ids[0], job_ids[2])

        self.assertEqual(res.returncode, 0)
        self.assertIn("Removed 2 job(s).", res.stdout)
        items = self._read_queue()
        self.assertEqual([item["cmd"] for item in items], ["echo two", "echo four"])

    def test_qdel_accepts_unique_job_id_prefix(self):
        self._run_cli("submit", "echo", "one")
        self._run_cli("submit", "echo", "two")
        job_ids = self._queued_job_ids()

        res = self._run_cli("qdel", job_ids[1][:-1])

        self.assertEqual(res.returncode, 0)
        self.assertIn("Removed 1 job(s).", res.stdout)
        self.assertEqual([item["cmd"] for item in self._read_queue()], ["echo one"])

    def test_qdel_still_deletes_by_index_behind_flag(self):
        self._run_cli("submit", "echo", "one")
        self._run_cli("submit", "echo", "two")
        self._run_cli("submit", "echo", "three")

        res = self._run_cli("qdel", "--index", "2")

        self.assertEqual(res.returncode, 0)
        self.assertIn("Removed 1 job(s).", res.stdout)
        self.assertEqual(
            [item["cmd"] for item in self._read_queue()], ["echo one", "echo three"]
        )

    def test_qdel_deletes_a_whole_queue(self):
        # `submit` only accepts configured queue names, so this covers the flag
        # end-to-end on `default`; selective removal is covered at the RPC layer
        # in test_host_rpc.QdelTests.
        self._run_cli("submit", "echo", "one")
        self._run_cli("submit", "echo", "two")

        res = self._run_cli("qdel", "--queue", "default")

        self.assertEqual(res.returncode, 0)
        self.assertIn("Removed 2 job(s).", res.stdout)
        self.assertEqual(self._read_queue(), [])

    def test_qdel_rejects_bare_integer_and_points_at_index_flag(self):
        self._run_cli("submit", "echo", "one")
        self._run_cli("submit", "echo", "two")

        res = self._run_cli("qdel", "2")

        self.assertEqual(res.returncode, 1)
        self.assertIn("looks like a queue index", res.stdout)
        self.assertIn("--index 2", res.stdout)
        self.assertEqual(
            [item["cmd"] for item in self._read_queue()], ["echo one", "echo two"]
        )

    def test_qdel_rejects_unknown_job_id_without_changes(self):
        self._run_cli("submit", "echo", "one")
        self._run_cli("submit", "echo", "two")

        res = self._run_cli("qdel", "nosuchjob")

        self.assertEqual(res.returncode, 1)
        self.assertIn("no queued job matching: nosuchjob", res.stdout)
        self.assertEqual(
            [item["cmd"] for item in self._read_queue()], ["echo one", "echo two"]
        )

    def test_qdel_rejects_combined_selectors(self):
        self._run_cli("submit", "echo", "one")
        job_ids = self._queued_job_ids()

        res = self._run_cli("qdel", job_ids[0], "--index", "1")

        self.assertEqual(res.returncode, 1)
        self.assertIn("Cannot combine", res.stdout)
        self.assertEqual([item["cmd"] for item in self._read_queue()], ["echo one"])

    def test_remote_qdel_forwards_job_ids_over_ssh(self):
        # Mirrors the test_remote_submit pattern: stub rpc_call and verify the
        # client builds the right RPC envelope rather than dispatching locally.
        class Args:
            queue_host = "queuebox"
            job_ids = ["JOB-A", "JOB-C"]
            indices = []
            queue = None

        captured = {}

        def fake_rpc_call(host, method, params, **kwargs):
            captured["queue_host"] = host
            captured["method"] = method
            captured["params"] = params
            return {
                "removed": [
                    {"index": 1, "item": {"job_id": "JOB-A", "cmd": "echo a", "priority": 0,
                                          "queue": "default", "hosts": None}},
                    {"index": 3, "item": {"job_id": "JOB-C", "cmd": "echo c", "priority": 5,
                                          "queue": "fast", "hosts": ["eci2"]}},
                ]
            }

        from awsqueueengine.client import cli as client_cli
        with patch("awsqueueengine.client.cli.rpc_call", side_effect=fake_rpc_call):
            client_cli.cmd_qdel_remote(Args())

        self.assertEqual(captured["queue_host"], "queuebox")
        self.assertEqual(captured["method"], "qdel")
        # Only the selector actually used goes on the wire.
        self.assertEqual(captured["params"], {"job_ids": ["JOB-A", "JOB-C"]})

    def test_remote_qdel_forwards_index_selector_unchanged(self):
        """`indices` keeps its original wire meaning, so old hosts still work."""
        class Args:
            queue_host = "queuebox"
            job_ids = []
            indices = [1, 3]
            queue = None

        captured = {}

        def fake_rpc_call(host, method, params, **kwargs):
            captured["params"] = params
            return {"removed": []}

        from awsqueueengine.client import cli as client_cli
        with patch("awsqueueengine.client.cli.rpc_call", side_effect=fake_rpc_call):
            client_cli.cmd_qdel_remote(Args())

        self.assertEqual(captured["params"], {"indices": [1, 3]})

    def test_remote_qdel_forwards_queue_selector(self):
        class Args:
            queue_host = "queuebox"
            job_ids = []
            indices = []
            queue = "fast"

        captured = {}

        def fake_rpc_call(host, method, params, **kwargs):
            captured["params"] = params
            return {"removed": []}

        from awsqueueengine.client import cli as client_cli
        with patch("awsqueueengine.client.cli.rpc_call", side_effect=fake_rpc_call):
            client_cli.cmd_qdel_remote(Args())

        self.assertEqual(captured["params"], {"queue": "fast"})

    def test_remote_qdel_rejects_missing_selectors(self):
        class Args:
            queue_host = "queuebox"
            job_ids = []
            indices = []
            queue = None

        from awsqueueengine.client import cli as client_cli
        with patch("awsqueueengine.client.cli.rpc_call") as rpc_mock:
            with self.assertRaises(SystemExit) as cm:
                client_cli.cmd_qdel_remote(Args())
        self.assertEqual(cm.exception.code, 1)
        rpc_mock.assert_not_called()

    def test_remote_qdel_hints_at_old_host_on_indices_error(self):
        """New client, old host: the host's `indices` complaint gets explained."""
        class Args:
            queue_host = "queuebox"
            job_ids = ["JOB-A"]
            indices = []
            queue = None

        from awsqueueengine.client import cli as client_cli
        from awsqueueengine.shared.protocol import RpcError

        err = RpcError("invalid_params", "indices must be a non-empty list")
        buf = io.StringIO()
        with patch("awsqueueengine.client.cli.rpc_call", side_effect=err):
            with contextlib.redirect_stderr(buf):
                with self.assertRaises(SystemExit) as cm:
                    client_cli.cmd_qdel_remote(Args())
        self.assertEqual(cm.exception.code, 1)
        self.assertIn("older awsqe-host", buf.getvalue())

    def test_status_fetches_host_pool_from_queue_host_beyond_20(self):
        # Regression: a client with no local queues.json must not fall back to
        # the built-in eci1..eci20 default for `status`. It asks the queue host
        # (the source of truth), which can have more than 20 hosts.
        class Args:
            queue_host = "queuebox"
            host_set = None
            hosts_file = None

        all_hosts = [f"eci{i}" for i in range(1, 31)]
        captured = {}

        def fake_rpc_call(host, method, params, **kwargs):
            captured["queue_host"] = host
            captured["method"] = method
            return {"queue_host_map": {"default": list(all_hosts)}}

        def fake_status_all(hosts):
            captured["probed"] = list(hosts)
            return [{"host": h, "reachable": True, "pid": None, "tag": None, "raw": ""} for h in hosts]

        from awsqueueengine.client import cli as client_cli
        with patch("awsqueueengine.client.cli.rpc_call", side_effect=fake_rpc_call), \
             patch("awsqueueengine.client.cli.effective_queue_host", return_value="queuebox"), \
             patch("awsqueueengine.client.cli.status_all", side_effect=fake_status_all):
            client_cli.cmd_status(Args())

        self.assertEqual(captured["queue_host"], "queuebox")
        self.assertEqual(captured["method"], "stats")
        self.assertEqual(set(captured["probed"]), set(all_hosts))
        self.assertIn("eci30", captured["probed"])  # the whole point: past eci20

    def test_status_falls_back_to_local_when_queue_host_unreachable(self):
        # If the queue host can't be reached, `status` warns and uses local host
        # resolution rather than failing outright.
        from awsqueueengine.shared.protocol import RpcTransportError

        class Args:
            queue_host = "queuebox"
            host_set = None
            hosts_file = None

        captured = {}

        def boom(host, method, params, **kwargs):
            raise RpcTransportError(255, "ssh: connect: Connection refused")

        def fake_status_all(hosts):
            captured["probed"] = list(hosts)
            return []

        from awsqueueengine.client import cli as client_cli
        with patch("awsqueueengine.client.cli.rpc_call", side_effect=boom), \
             patch("awsqueueengine.client.cli.effective_queue_host", return_value="queuebox"), \
             patch("awsqueueengine.client.cli._resolve_queue_hosts_for_cli",
                   return_value={"default": ["eci1", "eci2"]}), \
             patch("awsqueueengine.client.cli.status_all", side_effect=fake_status_all):
            client_cli.cmd_status(Args())

        self.assertEqual(captured["probed"], ["eci1", "eci2"])

    def test_status_hosts_file_override_bypasses_queue_host(self):
        # An explicit --hosts-file is a local override: no RPC to the queue host.
        class Args:
            queue_host = "queuebox"
            host_set = None
            hosts_file = "/tmp/does-not-matter"

        captured = {}

        def fake_status_all(hosts):
            captured["probed"] = list(hosts)
            return []

        from awsqueueengine.client import cli as client_cli
        with patch("awsqueueengine.client.cli.rpc_call") as rpc_mock, \
             patch("awsqueueengine.client.cli._resolve_queue_hosts_for_cli",
                   return_value={"default": ["eci1", "eci2", "eci3"]}), \
             patch("awsqueueengine.client.cli.status_all", side_effect=fake_status_all):
            client_cli.cmd_status(Args())

        rpc_mock.assert_not_called()
        self.assertEqual(captured["probed"], ["eci1", "eci2", "eci3"])

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

    def _write_client_config(self, body):
        cfg_dir = self.home_path / ".awsqe" / "client"
        cfg_dir.mkdir(parents=True, exist_ok=True)
        (cfg_dir / "config.toml").write_text(body)

    def test_legacy_list_uses_config_queue_host_when_flag_omitted(self):
        # With `~/.awsqe/client/config.toml` providing `queue_host`, the legacy
        # `awsqueueengine list` (no --queue-host) must route via SSH/RPC, not
        # read the local queue file.
        self._write_client_config('[default]\nqueue_host = "configbox"\n')
        capture_path = self.home_path / "ssh_args.txt"
        stdin_path = self.home_path / "ssh_stdin.txt"
        fake_ssh_dir = self._make_fake_ssh_rpc(capture_path, stdin_path, result={"jobs": []})

        res = self._run_cli_with_path_prefix(fake_ssh_dir, "list")

        self.assertEqual(res.returncode, 0, msg=res.stdout + res.stderr)
        captured = capture_path.read_text().splitlines()
        self.assertEqual(captured[0], "configbox")
        self.assertEqual(captured[1], "awsqe-host")
        self.assertEqual(captured[2], "rpc")
        request = json.loads(stdin_path.read_text())
        self.assertEqual(request["method"], "list")

    def test_legacy_list_falls_back_to_local_when_config_unset(self):
        # No config file → no queue_host → reads the local queue file as before.
        res = self._run_cli("list")

        self.assertEqual(res.returncode, 0)
        self.assertIn("(queue empty)", res.stdout)

    def test_cli_flag_overrides_config_queue_host(self):
        self._write_client_config('[default]\nqueue_host = "configbox"\n')
        capture_path = self.home_path / "ssh_args.txt"
        stdin_path = self.home_path / "ssh_stdin.txt"
        fake_ssh_dir = self._make_fake_ssh_rpc(capture_path, stdin_path, result={"jobs": []})

        res = self._run_cli_with_path_prefix(fake_ssh_dir, "list", "--queue-host", "flagbox")

        self.assertEqual(res.returncode, 0, msg=res.stdout + res.stderr)
        captured = capture_path.read_text().splitlines()
        self.assertEqual(captured[0], "flagbox")


if __name__ == "__main__":
    unittest.main()
