"""Client-side surface for failed jobs: `awsqe-client failed` and `info`."""
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from awsqueueengine.client import cli as client_cli
from awsqueueengine.client import ledger as ledger_mod


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
        # `info` writes back to the tracked-job ledger; keep that off the real $HOME.
        self._original_ledger = ledger_mod.LEDGER_PATH
        ledger_mod.LEDGER_PATH = self.payload / "jobs.json"

    def tearDown(self):
        ledger_mod.LEDGER_PATH = self._original_ledger
        self.tmpdir.cleanup()

    def _run_info(self, state):
        # Deliberately no `job_id` attribute: the legacy `awsqueueengine info`
        # shim declares no --job-id and calls cmd_info directly, so this also
        # pins the getattr fallback.
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


class InfoByJobIdTests(unittest.TestCase):
    """`info --job-id` resolves through the ledger, with no payload dir needed."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self._original_ledger = ledger_mod.LEDGER_PATH
        ledger_mod.LEDGER_PATH = self.root / "jobs.json"

    def tearDown(self):
        ledger_mod.LEDGER_PATH = self._original_ledger
        self.tmpdir.cleanup()

    def _args(self, **kwargs):
        class Args:
            payload = None
            queue_host = None
            job_id = None
        args = Args()
        for key, value in kwargs.items():
            setattr(args, key, value)
        return args

    def _run(self, args, state):
        buffer = io.StringIO()
        with patch("awsqueueengine.client.cli.rpc_call", return_value={"state": state}):
            with redirect_stdout(buffer):
                client_cli.cmd_info(args)
        return buffer.getvalue()

    def test_parser_wires_job_id(self):
        args = client_cli.build_parser().parse_args(["info", "--job-id", "JOB-A"])
        self.assertEqual(args.job_id, "JOB-A")

    def test_resolves_a_tracked_job_with_no_payload_dir(self):
        ledger_mod.record_submission(job_id="JOB-A", queue_host="queuebox", cmd="run.sh")
        output = self._run(self._args(job_id="JOB-A"),
                           {"status": "running", "job_id": "JOB-A", "host": "eci7"})
        self.assertIn("Job JOB-A", output)
        self.assertIn("status=running", output)

    def test_accepts_a_unique_prefix(self):
        ledger_mod.record_submission(job_id="20260730-141530-a1b2c3", queue_host="queuebox")
        output = self._run(self._args(job_id="20260730-1415"), {"status": "queued"})
        self.assertIn("status=queued", output)

    def test_writes_state_back_to_the_ledger(self):
        ledger_mod.record_submission(job_id="JOB-A", queue_host="queuebox")
        self._run(self._args(job_id="JOB-A"),
                  {"status": "failed", "failure_reason": "segfault", "exit_code": "139"})
        record = ledger_mod.load_ledger()[0]
        self.assertEqual(record["status"], "failed")
        self.assertEqual(record["failure_reason"], "segfault")

    def test_rewrites_run_info_when_the_recorded_payload_still_exists(self):
        payload = self.root / "run"
        payload.mkdir()
        (payload / "run.info").write_text("job_id: JOB-A\n")
        ledger_mod.record_submission(job_id="JOB-A", queue_host="queuebox", payload=str(payload))
        output = self._run(self._args(job_id="JOB-A"), {"status": "completed", "host": "eci3"})
        self.assertIn(f"Updated {payload / 'run.info'}", output)
        self.assertIn("status: completed", (payload / "run.info").read_text())

    def test_a_vanished_payload_dir_is_not_an_error(self):
        ledger_mod.record_submission(job_id="JOB-A", queue_host="queuebox",
                                     payload=str(self.root / "gone"))
        output = self._run(self._args(job_id="JOB-A"), {"status": "completed"})
        self.assertIn("Job JOB-A", output)

    def test_untracked_job_id_exits_1_and_points_at_jobs(self):
        buffer = io.StringIO()
        with self.assertRaises(SystemExit) as ctx, redirect_stdout(buffer):
            client_cli.cmd_info(self._args(job_id="NOPE"))
        self.assertEqual(ctx.exception.code, 1)
        self.assertIn("awsqe-client jobs", buffer.getvalue())

    def test_ambiguous_prefix_exits_1(self):
        ledger_mod.record_submission(job_id="20260730-141530-a1b2c3", queue_host="qh")
        ledger_mod.record_submission(job_id="20260730-141530-d4e5f6", queue_host="qh")
        buffer = io.StringIO()
        with self.assertRaises(SystemExit) as ctx, redirect_stdout(buffer):
            client_cli.cmd_info(self._args(job_id="20260730-1415"))
        self.assertEqual(ctx.exception.code, 1)
        self.assertIn("matches 2 tracked jobs", buffer.getvalue())

    def test_info_never_starts_tracking_an_untracked_job(self):
        """Inspecting a payload from another machine must not pollute the ledger."""
        payload = self.root / "someone-elses"
        payload.mkdir()
        (payload / "run.info").write_text("job_id: FOREIGN\nqueue_host: queuebox\n")
        self._run(self._args(payload=str(payload)), {"status": "completed"})
        self.assertEqual(ledger_mod.load_ledger(), [])

    def test_can_recheck_a_job_the_ledger_considers_terminal(self):
        """`jobs` skips terminal records on refresh; `info --job-id` is the way back in."""
        ledger_mod.record_submission(job_id="JOB-A", queue_host="queuebox")
        ledger_mod.apply_state("JOB-A", {"status": "failed", "failure_reason": "nonzero_exit"})
        self._run(self._args(job_id="JOB-A"), {"status": "running", "host": "eci7"})
        record = ledger_mod.load_ledger()[0]
        self.assertEqual(record["status"], "running")
        self.assertNotIn("failure_reason", record)


if __name__ == "__main__":
    unittest.main()
