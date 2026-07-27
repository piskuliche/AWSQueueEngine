"""Client-side surface for failed jobs: `awsqe-client failed` and `info`."""
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from awsqueueengine.client import cli as client_cli


FAILED_JOB = {
    "job_id": "JOB-A",
    "host": "eci7",
    "queue": "gpu",
    "cmd": "bash run.sh",
    "dur": "00:00:04",
    "exit_code": 1,
    "status": "failed",
    "failure_reason": "out_of_memory",
    "failure_detail": "RuntimeError: CUDA error: out of memory",
    "log_tail": "loading inputs\nRuntimeError: CUDA error: out of memory",
    "failed_at": 1785000000.0,
}


class FailedSubcommandTests(unittest.TestCase):
    def test_parser_wires_failed_subcommand(self):
        args = client_cli.build_parser().parse_args(["failed", "-n", "5", "--job-id", "JOB-A", "--log"])
        self.assertEqual(args.cmd, "failed")
        self.assertEqual(args.limit, 5)
        self.assertEqual(args.job_id, "JOB-A")
        self.assertTrue(args.log)

    def test_failed_defaults_to_50_without_log_or_filter(self):
        args = client_cli.build_parser().parse_args(["failed"])
        self.assertEqual(args.limit, 50)
        self.assertIsNone(args.job_id)
        self.assertFalse(args.log)

    def test_failed_forwards_params_and_renders_reason(self):
        class Args:
            queue_host = "queuebox"
            limit = 10
            job_id = "JOB-A"
            log = True

        captured = {}

        def fake_rpc_call(host, method, params, **kwargs):
            captured.update({"host": host, "method": method, "params": params})
            return {"jobs": [FAILED_JOB]}

        buffer = io.StringIO()
        with patch("awsqueueengine.client.cli.rpc_call", side_effect=fake_rpc_call):
            with redirect_stdout(buffer):
                client_cli.cmd_failed_remote(Args())

        self.assertEqual(captured["host"], "queuebox")
        self.assertEqual(captured["method"], "failed_list")
        self.assertEqual(captured["params"], {"limit": 10, "log": True, "job_id": "JOB-A"})
        output = buffer.getvalue()
        self.assertIn("JOB-A", output)
        self.assertIn("out_of_memory", output)
        self.assertIn("RuntimeError: CUDA error: out of memory", output)
        # --log echoes the captured tail.
        self.assertIn("|  loading inputs", output)

    def test_failed_without_log_flag_omits_the_tail(self):
        class Args:
            queue_host = "queuebox"
            limit = 50
            job_id = None
            log = False

        record = dict(FAILED_JOB)
        record.pop("log_tail")
        buffer = io.StringIO()
        with patch("awsqueueengine.client.cli.rpc_call", return_value={"jobs": [record]}):
            with redirect_stdout(buffer):
                client_cli.cmd_failed_remote(Args())

        self.assertNotIn("loading inputs", buffer.getvalue())

    def test_empty_history_renders_a_clear_message(self):
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            client_cli._render_failed_jobs([])
        self.assertIn("(no failed jobs recorded)", buffer.getvalue())


class InfoFailureReportingTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.payload = Path(self.tmpdir.name)
        (self.payload / "run.info").write_text("job_id: JOB-A\nqueue_host: queuebox\n")

    def tearDown(self):
        self.tmpdir.cleanup()

    def _run_info(self, state):
        class Args:
            payload = str(self.payload)
            queue_host = None

        buffer = io.StringIO()
        with patch("awsqueueengine.client.cli.rpc_call", return_value={"state": state}):
            with redirect_stdout(buffer):
                client_cli.cmd_info(Args())
        return buffer.getvalue(), (self.payload / "run.info").read_text()

    def test_info_reports_failure_reason_and_persists_it(self):
        output, run_info = self._run_info({
            "status": "failed",
            "job_id": "JOB-A",
            "host": "eci7",
            "failure_reason": "segfault",
            "failure_detail": "Segmentation fault (core dumped)",
            "exit_code": "139",
        })
        self.assertIn("status=failed", output)
        self.assertIn("reason=segfault exit=139", output)
        self.assertIn("Segmentation fault (core dumped)", output)
        self.assertIn("failure_reason: segfault", run_info)
        self.assertIn("exit_code: 139", run_info)

    def test_successful_retry_clears_stale_failure_fields(self):
        (self.payload / "run.info").write_text(
            "job_id: JOB-A\nqueue_host: queuebox\nstatus: failed\n"
            "failure_reason: segfault\nfailure_detail: boom\nexit_code: 139\n"
        )
        output, run_info = self._run_info({
            "status": "completed",
            "job_id": "JOB-A",
            "host": "eci7",
            "duration": "01:00:00",
        })
        self.assertIn("status=completed", output)
        self.assertNotIn("failure_reason", run_info)
        self.assertNotIn("exit_code", run_info)


if __name__ == "__main__":
    unittest.main()
