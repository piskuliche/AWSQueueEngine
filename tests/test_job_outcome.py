import unittest

from awsqueueengine.shared import job_outcome
from awsqueueengine.shared.job_outcome import classify_failure, fetch_job_outcome


def _fake_ssh(out, rc=0, err="", recorder=None):
    def run(host, cmd, *args, **kwargs):
        if recorder is not None:
            recorder.append((host, cmd))
        return rc, out, err
    return run


class FetchJobOutcomeTests(unittest.TestCase):
    def test_parses_exit_code_and_log_tail(self):
        calls = []
        outcome = fetch_job_outcome(
            "eci4",
            "tag-1",
            ssh_run=_fake_ssh("3\n__awsqe_log__\nsome output\nboom\n", recorder=calls),
        )
        self.assertTrue(outcome["found"])
        self.assertEqual(outcome["exit_code"], 3)
        self.assertEqual(outcome["log_tail"], "some output\nboom")
        self.assertIn("tag-1.rc", calls[0][1])
        self.assertIn("tag-1.log", calls[0][1])

    def test_missing_status_file_is_reported_not_guessed(self):
        outcome = fetch_job_outcome(
            "eci4", "tag-1", ssh_run=_fake_ssh("__awsqe_no_rc__\n__awsqe_log__\nlog line\n")
        )
        self.assertFalse(outcome["found"])
        self.assertIsNone(outcome["exit_code"])
        self.assertEqual(outcome["log_tail"], "log line")
        self.assertIn("no exit status", outcome["error"])

    def test_unreachable_host_reports_the_ssh_error(self):
        outcome = fetch_job_outcome("eci4", "tag-1", ssh_run=_fake_ssh("", rc=255, err="connection refused"))
        self.assertFalse(outcome["found"])
        self.assertEqual(outcome["error"], "connection refused")

    def test_missing_tag_short_circuits_without_ssh(self):
        calls = []
        outcome = fetch_job_outcome("eci4", None, ssh_run=_fake_ssh("0\n__awsqe_log__\n", recorder=calls))
        self.assertFalse(outcome["found"])
        self.assertEqual(calls, [])
        self.assertIn("no job tag", outcome["error"])

    def test_unparseable_status_is_not_treated_as_success(self):
        outcome = fetch_job_outcome("eci4", "tag-1", ssh_run=_fake_ssh("garbage\n__awsqe_log__\n"))
        self.assertFalse(outcome["found"])
        self.assertIsNone(outcome["exit_code"])
        self.assertIn("unparseable", outcome["error"])

    def test_line_count_is_clamped(self):
        calls = []
        fetch_job_outcome("eci4", "tag-1", lines=100000, ssh_run=_fake_ssh("0\n__awsqe_log__\n", recorder=calls))
        self.assertIn("tail -n 500 ", calls[0][1])


class ClassifyFailureTests(unittest.TestCase):
    def test_log_evidence_beats_the_exit_code(self):
        reason, detail = classify_failure(1, "starting\ntorch: CUDA error: out of memory\n")
        self.assertEqual(reason, "out_of_memory")
        self.assertIn("CUDA error", detail)

    def test_disk_full_detected(self):
        reason, _ = classify_failure(1, "cp: writing 'x': No space left on device")
        self.assertEqual(reason, "disk_full")

    def test_cuda_error_detected(self):
        reason, _ = classify_failure(1, "CUDA error: no CUDA-capable device is detected")
        self.assertEqual(reason, "cuda_error")

    def test_python_traceback_detected(self):
        reason, _ = classify_failure(1, "Traceback (most recent call last):\n  File ...")
        self.assertEqual(reason, "python_exception")

    def test_exit_code_fallbacks(self):
        self.assertEqual(classify_failure(127, "")[0], "command_not_found")
        self.assertEqual(classify_failure(126, "")[0], "not_executable")
        self.assertEqual(classify_failure(137, "")[0], "killed")
        self.assertEqual(classify_failure(139, "")[0], "segfault")
        self.assertEqual(classify_failure(1, "")[0], "nonzero_exit")
        self.assertEqual(classify_failure(140, "")[0], "signal_12")

    def test_missing_exit_code_uses_no_exit_status(self):
        reason, detail = classify_failure(None, "", error="host went away")
        self.assertEqual(reason, "no_exit_status")
        self.assertEqual(detail, "host went away")

    def test_detail_falls_back_to_last_log_line_then_exit_status(self):
        _reason, detail = classify_failure(2, "step one\nstep two\n\n")
        self.assertEqual(detail, "step two")
        _reason, detail = classify_failure(2, "")
        self.assertEqual(detail, "exited with status 2")

    def test_detail_is_single_line_and_bounded(self):
        _reason, detail = classify_failure(1, "x" * 5000)
        self.assertLessEqual(len(detail), job_outcome.MAX_DETAIL_CHARS)
        self.assertNotIn("\n", detail)


if __name__ == "__main__":
    unittest.main()
