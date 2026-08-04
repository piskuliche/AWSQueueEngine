import os
import unittest
from pathlib import Path
from unittest.mock import patch

from awsqueueengine.host.job_control import (
    LAUNCH_SCRIPT_TEMPLATE,
    _build_launch_command,
    submit_to_host,
    wrap_in_mps_script,
)
from awsqueueengine.shared.worker_actions import kill_managed_on_host
from awsqueueengine.shared.config import REMOTE_LOG_DIR


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

        with patch("awsqueueengine.host.job_control.choose_scratch_on_host", return_value=("/scratch", 1000)), patch(
            "awsqueueengine.host.job_control.ssh_run", side_effect=fake_ssh_run
        ):
            result = submit_to_host(
                "eci5",
                "bash run.sh",
                payload_s3_uri="s3://bucket/payload.tar.gz",
                payload_size_bytes=500,
            )

        self.assertTrue(result["ok"])
        self.assertRegex(result["tag"], r"^\d{8}-\d{6}-[0-9a-f]{6}$")
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

        with patch("awsqueueengine.host.job_control.choose_scratch_on_host", return_value=("/scratch", 1000)), patch(
            "awsqueueengine.host.job_control.ssh_run", side_effect=fake_ssh_run
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


class MpsWrapTests(unittest.TestCase):
    def test_wrap_places_command_between_mps_launch_and_teardown(self):
        wrapped = wrap_in_mps_script("bash run.sh --epochs 5", job_name="20260616-120000-abcdef")

        # Command lands where AAA was, bracketed by MPS launch + teardown.
        self.assertIn("nvidia-cuda-mps-control -d", wrapped)
        self.assertIn("bash run.sh --epochs 5", wrapped)
        self.assertIn("echo quit | nvidia-cuda-mps-control", wrapped)
        launch_idx = wrapped.index("nvidia-cuda-mps-control -d")
        cmd_idx = wrapped.index("bash run.sh --epochs 5")
        teardown_idx = wrapped.index("echo quit | nvidia-cuda-mps-control")
        cleanup_idx = wrapped.index("rm -rf ${temp_path}")
        self.assertLess(launch_idx, cmd_idx)
        self.assertLess(cmd_idx, teardown_idx)
        # Per-job /tmp scratch is removed last so it doesn't accumulate.
        self.assertLess(teardown_idx, cleanup_idx)
        # job_name drives the per-job MPS pipe/log directories.
        self.assertIn("job_name=20260616-120000-abcdef", wrapped)
        self.assertIn("export CUDA_MPS_PIPE_DIRECTORY=${temp_path}/nvidia-mps", wrapped)

    def test_submit_to_host_wraps_command_when_mps_enabled(self):
        commands = []

        def fake_ssh_run(_host, cmd, timeout=60, capture_output=True):
            commands.append(cmd)
            if cmd.startswith("cat "):
                return 0, "1234", ""
            if "ps -p 1234" in cmd:
                return 0, "1234", ""
            return 0, "", ""

        with patch("awsqueueengine.host.job_control.ssh_run", side_effect=fake_ssh_run):
            result = submit_to_host("eci5", "bash run.sh", tag="20260616-120000-abcdef", mps=True)

        self.assertTrue(result["ok"])
        launch_cmd = commands[0]
        self.assertIn("nvidia-cuda-mps-control -d", launch_cmd)
        self.assertIn("bash run.sh", launch_cmd)
        self.assertIn("echo quit | nvidia-cuda-mps-control", launch_cmd)
        # The MANAGER_TAG env wrapper and pidfile plumbing are still present.
        self.assertIn("MANAGER_TAG=20260616-120000-abcdef", launch_cmd)

    def test_submit_to_host_does_not_wrap_when_mps_disabled(self):
        commands = []

        def fake_ssh_run(_host, cmd, timeout=60, capture_output=True):
            commands.append(cmd)
            if cmd.startswith("cat "):
                return 0, "1234", ""
            if "ps -p 1234" in cmd:
                return 0, "1234", ""
            return 0, "", ""

        with patch("awsqueueengine.host.job_control.ssh_run", side_effect=fake_ssh_run):
            submit_to_host("eci5", "bash run.sh", tag="20260616-120000-abcdef")

        self.assertNotIn("nvidia-cuda-mps-control", commands[0])

    def test_mps_wrapper_preserves_the_job_exit_status(self):
        wrapped = wrap_in_mps_script("bash run.sh", job_name="tag-1")
        # The job's status is captured before MPS teardown and re-raised, so the
        # outer .rc file records the job's result, not `rm -rf`'s.
        self.assertLess(wrapped.index("__awsqe_job_rc=$?"), wrapped.index("echo quit"))
        self.assertTrue(wrapped.rstrip().endswith("exit $__awsqe_job_rc"))


class ExitStatusCaptureTests(unittest.TestCase):
    def _submit_and_capture(self, **kwargs):
        commands = []

        def fake_ssh_run(_host, cmd, timeout=60, capture_output=True):
            commands.append(cmd)
            if cmd.startswith("cat "):
                return 0, "1234", ""
            if "ps -p 1234" in cmd:
                return 0, "1234", ""
            return 0, "", ""

        with patch("awsqueueengine.host.job_control.ssh_run", side_effect=fake_ssh_run):
            submit_to_host("eci5", "bash run.sh", tag="20260616-120000-abcdef", **kwargs)
        return commands[0]

    def test_launch_command_records_exit_status_next_to_the_log(self):
        launch_cmd = self._submit_and_capture()
        self.assertIn("/manager_jobs/20260616-120000-abcdef.rc", launch_cmd)
        self.assertIn("__awsqe_rc=$?", launch_cmd)
        # Stale status from a previous attempt with the same tag is cleared first.
        self.assertLess(launch_cmd.index("rm -f "), launch_cmd.index("bash run.sh"))

    def test_exit_status_is_recorded_for_mps_jobs_too(self):
        launch_cmd = self._submit_and_capture(mps=True)
        self.assertIn("/manager_jobs/20260616-120000-abcdef.rc", launch_cmd)
        self.assertIn("nvidia-cuda-mps-control -d", launch_cmd)


class LaunchCommandShapeTests(unittest.TestCase):
    """The launch must hand the job off and return, not wait on it (issue #33)."""

    def _launch_cmd_for(self, job_command, **kwargs):
        commands = []

        def fake_ssh_run(_host, cmd, timeout=60, capture_output=True):
            commands.append(cmd)
            if cmd.startswith("cat "):
                return 0, "1234", ""
            if "ps -p 1234" in cmd:
                return 0, "1234", ""
            return 0, "", ""

        with patch("awsqueueengine.host.job_control.choose_scratch_on_host", return_value=("/scratch", 1000)), patch(
            "awsqueueengine.host.job_control.ssh_run", side_effect=fake_ssh_run
        ):
            submit_to_host("eci5", job_command, tag="20260616-120000-abcdef", **kwargs)
        return commands[0]

    def _launch_cmd(self, **kwargs):
        return self._launch_cmd_for("bash run.sh", **kwargs)

    def test_no_and_chain_for_the_ampersand_to_swallow(self):
        # The bug: written as `mkdir && cd || true && nohup ... &`, the trailing
        # `&` binds looser than `&&` and backgrounds the *whole* chain, whose
        # subshell then holds ssh's stdout open for the job's entire runtime.
        # With mkdir/cd as their own statements there is no chain left to
        # swallow, so `&` can only apply to the nohup.
        #
        # Asserted against the template, whose only variable is the job command
        # placeholder. The rendered script embeds the caller's command verbatim,
        # and that command is free to contain `&&` with no bearing on this.
        scaffold = LAUNCH_SCRIPT_TEMPLATE.replace("{job_command}", "JOB")
        self.assertNotIn("&&", scaffold)
        nohup_line = next(l for l in scaffold.splitlines() if l.startswith("nohup "))
        self.assertTrue(nohup_line.rstrip().endswith("&"))
        mkdir_line = next(l for l in scaffold.splitlines() if l.startswith("mkdir -p "))
        self.assertNotIn("&", mkdir_line)

    def test_a_job_command_containing_and_chains_is_left_alone(self):
        # A job command with its own `&&` must survive untouched -- and must not
        # be mistaken for the launch scaffolding's control flow.
        launch_cmd = self._launch_cmd_for("make && make test")
        self.assertIn("make && make test", launch_cmd)
        self.assertEqual(launch_cmd.strip().splitlines()[-1], "exit 0")

    def test_stale_pidfile_from_a_previous_attempt_is_cleared(self):
        # Requeued jobs reuse their tag, so a pidfile left behind by an earlier
        # attempt must not be readable as this attempt's result.
        launch_cmd = self._launch_cmd()
        self.assertIn("rm -f /home/ubuntu/manager_jobs/20260616-120000-abcdef.pid", launch_cmd)
        self.assertLess(launch_cmd.index("rm -f "), launch_cmd.index("nohup "))

    def test_job_is_detached_into_its_own_session(self):
        self.assertIn("nohup setsid env MANAGER_TAG=", self._launch_cmd())

    def test_shell_exits_immediately_so_ssh_returns(self):
        self.assertEqual(self._launch_cmd().strip().splitlines()[-1], "exit 0")

    def test_pidfile_is_written_after_the_directory_exists(self):
        launch_cmd = self._launch_cmd()
        # `echo $! > .../tag.pid` used to run in the foreground, racing the
        # backgrounded `mkdir` that creates the directory it writes into.
        self.assertLess(launch_cmd.index("mkdir -p "), launch_cmd.index("echo \"$__awsqe_pid\""))

    def test_recorded_pid_is_the_job_not_a_wrapper_subshell(self):
        launch_cmd = self._launch_cmd()
        nohup_line = next(l for l in launch_cmd.splitlines() if "nohup" in l)
        pid_capture = next(l for l in launch_cmd.splitlines() if "__awsqe_pid=$!" in l)
        self.assertLess(launch_cmd.index(nohup_line), launch_cmd.index(pid_capture))

    def test_payload_path_shares_the_same_launch_shape(self):
        launch_cmd = self._launch_cmd(payload_remote_path="/scratch/payload-1")
        self.assertIn("PAYLOAD_DIR=/scratch/payload-1", launch_cmd)
        self.assertIn("nohup setsid env MANAGER_TAG=", launch_cmd)
        self.assertEqual(launch_cmd.strip().splitlines()[-1], "exit 0")


class LaunchReturnsImmediatelyTests(unittest.TestCase):
    """Run the generated script through a real bash, the way sshd would.

    The unit tests above assert the script's *shape*; this asserts the property
    that shape exists for. Before the fix this took the job's full runtime
    instead of returning at once, which is what produced the bogus `ssh
    timeout` (issue #33).
    """

    def test_launch_returns_while_the_job_is_still_running(self):
        import subprocess
        import tempfile
        import time

        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "job-finished"
            # Kept short: the job outlives this block by design, so a long sleep
            # would leave a detached process writing its log into a directory
            # that has already been torn down.
            script = _build_launch_command(
                "20260616-120000-abcdef",
                f"sleep 2; touch {marker}",
            ).replace(REMOTE_LOG_DIR, tmp)

            started = time.monotonic()
            proc = subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=30)
            elapsed = time.monotonic() - started

            self.assertEqual(proc.returncode, 0, proc.stderr)
            # The job sleeps 2s; the launch must not wait for it. capture_output
            # reads stdout to EOF, which is what ssh does with its channel --
            # the old shape blocked here for the job's full runtime.
            self.assertLess(elapsed, 1.5, f"launch blocked for {elapsed:.1f}s")
            self.assertFalse(marker.exists(), "launch waited for the job to finish")

            pid = (Path(tmp) / "20260616-120000-abcdef.pid").read_text().strip()
            self.assertTrue(pid.isdigit())
            # The recorded pid is the job itself, not a wrapper subshell that
            # exits immediately -- it is still alive right after the launch.
            os.kill(int(pid), 0)


class LaunchTimeoutVerificationTests(unittest.TestCase):
    """A launch that timed out but actually started must not blame the host."""

    @staticmethod
    def _fake_ssh(launch_rc=124, launch_err="ssh timeout", pidfile="1234", process_alive=True, scan=""):
        def fake_ssh_run(_host, cmd, timeout=60, capture_output=True):
            if cmd.startswith("cat "):
                return 0, pidfile, ""
            if cmd.startswith("ps -p "):
                return (0, "1234", "") if process_alive else (0, "", "")
            if cmd.startswith("ps -eo"):
                return 0, scan, ""
            if "aws s3 cp" in cmd:
                return 0, "", ""
            return launch_rc, "", launch_err

        return fake_ssh_run

    def test_no_payload_timeout_on_a_running_job_reports_success(self):
        # The outage: reruns submitted without a payload outlived SSH_TIMEOUT,
        # so the launch ssh returned 124 even though the job was running fine.
        with patch("awsqueueengine.host.job_control.ssh_run", side_effect=self._fake_ssh()):
            result = submit_to_host("eci20", "bash run.sh", tag="20260803-163409-f4c156")

        self.assertTrue(result["ok"])
        self.assertEqual(result["pid"], "1234")
        self.assertNotIn("reason", result)

    def test_payload_timeout_on_a_running_job_reports_success(self):
        with patch("awsqueueengine.host.job_control.ssh_run", side_effect=self._fake_ssh()):
            result = submit_to_host("eci20", "bash run.sh", payload_remote_path="/scratch/p-1")

        self.assertTrue(result["ok"])
        self.assertEqual(result["payload"], "/scratch/p-1")

    def test_unreachable_host_with_nothing_running_is_host_transport(self):
        def fake_ssh_run(_host, _cmd, timeout=60, capture_output=True):
            return 255, "", "connection refused"

        with patch("awsqueueengine.host.job_control.ssh_run", side_effect=fake_ssh_run):
            result = submit_to_host("eci20", "bash run.sh")

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "host_transport")

    def test_reachable_host_with_nothing_running_is_a_job_failure(self):
        # The host answered both probes and simply has no such process, so the
        # job is at fault. Blaming the host here would cost it a cooldown.
        with patch(
            "awsqueueengine.host.job_control.ssh_run",
            side_effect=self._fake_ssh(launch_rc=1, launch_err="boom", pidfile="", scan=""),
        ):
            result = submit_to_host("eci20", "bash run.sh")

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "job")

    def test_dead_process_behind_a_pidfile_is_a_job_failure(self):
        with patch(
            "awsqueueengine.host.job_control.ssh_run",
            side_effect=self._fake_ssh(process_alive=False),
        ):
            result = submit_to_host("eci20", "bash run.sh")

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "job")
        self.assertIn("process not running", result["err"])

    def test_fallback_scan_is_scoped_to_this_tag(self):
        seen = []

        def fake_ssh_run(_host, cmd, timeout=60, capture_output=True):
            seen.append(cmd)
            if cmd.startswith("cat "):
                return 0, "", ""
            return 0, "", ""

        with patch("awsqueueengine.host.job_control.ssh_run", side_effect=fake_ssh_run):
            submit_to_host("eci20", "bash run.sh", tag="20260803-163409-f4c156")

        scan = next(c for c in seen if c.startswith("ps -eo"))
        # Unscoped, the scan reports another job's pid as ours. The bracket
        # keeps the probe from matching its own command line, so the tag is
        # scoped as `[M]ANAGER_TAG=<tag>`.
        self.assertIn("[M]ANAGER_TAG=20260803-163409-f4c156", scan)

    def test_a_torn_pidfile_is_not_interpolated_into_a_remote_command(self):
        seen = []

        def fake_ssh_run(_host, cmd, timeout=60, capture_output=True):
            seen.append(cmd)
            if cmd.startswith("cat "):
                return 0, "not-a-pid; rm -rf /", ""
            return 0, "", ""

        with patch("awsqueueengine.host.job_control.ssh_run", side_effect=fake_ssh_run):
            result = submit_to_host("eci20", "bash run.sh")

        self.assertFalse(any(c.startswith("ps -p ") for c in seen))
        self.assertIsNone(result["pid"])

    def test_probes_get_room_to_answer_on_a_slow_host(self):
        timeouts = {}

        def fake_ssh_run(_host, cmd, timeout=60, capture_output=True):
            if cmd.startswith("cat "):
                timeouts["cat"] = timeout
                return 0, "1234", ""
            if cmd.startswith("ps -p "):
                timeouts["ps"] = timeout
                return 0, "1234", ""
            return 124, "", "ssh timeout"

        with patch("awsqueueengine.host.job_control.ssh_run", side_effect=fake_ssh_run):
            submit_to_host("eci20", "bash run.sh")

        # This path exists for hosts too slow to answer within SSH_TIMEOUT, so a
        # tight probe budget would recreate the misattribution being fixed.
        self.assertGreaterEqual(timeouts["cat"], 15)
        self.assertGreaterEqual(timeouts["ps"], 15)

    def test_missing_pidfile_falls_back_to_the_manager_tag_scan(self):
        with patch(
            "awsqueueengine.host.job_control.ssh_run",
            side_effect=self._fake_ssh(pidfile="", scan="4321 bash -lc MANAGER_TAG=x"),
        ):
            result = submit_to_host("eci20", "bash run.sh")

        self.assertTrue(result["ok"])
        self.assertEqual(result["pid"], "4321")
        self.assertEqual(result["note"], "started-no-pidfile")


if __name__ == "__main__":
    unittest.main()
