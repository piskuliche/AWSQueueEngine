"""Tests for the local cache of worker job logs."""
import tempfile
import unittest
from pathlib import Path

from awsqueueengine.client import logs as logs_mod
from awsqueueengine.client.logs import (
    cache_key,
    fetch_log,
    forget_logs,
    is_cached,
    local_log_path,
    prune_log_cache,
    should_fetch,
)


def _fake_runner(returncode=0, stderr="", writes=None):
    """Stand in for scp: records argv and optionally creates the destination."""
    calls = []

    def runner(argv, timeout):
        calls.append(argv)
        if returncode == 0 and writes is not None:
            Path(argv[-1]).write_text(writes)
        return {"returncode": returncode, "stderr": stderr}

    runner.calls = calls
    return runner


class _LogFixture(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name) / "logs"
        self._original = logs_mod.LOG_DIR
        logs_mod.LOG_DIR = self.root

    def tearDown(self):
        logs_mod.LOG_DIR = self._original
        self.tmpdir.cleanup()

    def _record(self, **kwargs):
        base = {"job_id": "J1", "host": "eci7", "status": "completed",
                "finished_at": "2026-07-31 18:10:14"}
        base.update(kwargs)
        return base


class CacheKeyTests(_LogFixture):
    def test_key_changes_when_the_job_reruns_on_another_host(self):
        """A requeue truncates the remote log and may move hosts."""
        first = cache_key(self._record(host="eci3"))
        second = cache_key(self._record(host="eci7"))
        self.assertNotEqual(first, second)

    def test_key_changes_when_the_job_reruns_on_the_same_host(self):
        first = cache_key(self._record(finished_at="2026-07-31 18:10:14"))
        second = cache_key(self._record(finished_at="2026-08-01 09:00:00"))
        self.assertNotEqual(first, second)

    def test_key_is_stable_for_the_same_attempt(self):
        self.assertEqual(cache_key(self._record()), cache_key(self._record()))


class ShouldFetchTests(_LogFixture):
    def test_no_worker_recorded_means_nothing_to_fetch(self):
        self.assertFalse(should_fetch(self._record(host="", status="queued")))

    def test_terminal_job_without_a_cached_log_is_fetched(self):
        self.assertTrue(should_fetch(self._record()))

    def test_terminal_job_with_this_attempts_log_is_skipped(self):
        record = self._record()
        path = local_log_path("J1", root=self.root)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x")
        record.update(log_path=str(path), log_fetched_for=cache_key(record))
        self.assertTrue(is_cached(record))
        self.assertFalse(should_fetch(record))

    def test_a_cached_log_from_a_previous_attempt_is_refetched(self):
        """The whole point of keying on the attempt rather than the job id."""
        record = self._record(host="eci3")
        path = local_log_path("J1", root=self.root)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("old attempt")
        record.update(log_path=str(path), log_fetched_for=cache_key(record))
        record["host"] = "eci7"          # requeued elsewhere
        self.assertFalse(is_cached(record))
        self.assertTrue(should_fetch(record))

    def test_a_vanished_cache_file_is_refetched(self):
        record = self._record(log_path=str(self.root / "gone.log"))
        record["log_fetched_for"] = cache_key(record)
        self.assertFalse(is_cached(record))
        self.assertTrue(should_fetch(record))

    def test_running_jobs_are_always_refetched_because_the_log_is_still_growing(self):
        record = self._record(status="running")
        path = local_log_path("J1", root=self.root)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("partial")
        record.update(log_path=str(path), log_fetched_for=cache_key(record))
        self.assertTrue(should_fetch(record))

    def test_a_log_known_missing_for_this_attempt_is_not_retried(self):
        record = self._record()
        record["log_missing_for"] = cache_key(record)
        self.assertFalse(should_fetch(record))

    def test_but_a_rerun_makes_us_look_again(self):
        record = self._record(host="eci3")
        record["log_missing_for"] = cache_key(record)
        record["host"] = "eci7"
        self.assertTrue(should_fetch(record))


class FetchLogTests(_LogFixture):
    def test_successful_fetch_writes_the_file_and_reports_its_path(self):
        runner = _fake_runner(writes="log contents\n")
        result = fetch_log("J1", "eci7", root=self.root, runner=runner)
        self.assertTrue(result["ok"])
        self.assertEqual(Path(result["path"]).read_text(), "log contents\n")

    def test_scp_argv_targets_the_deterministic_remote_path(self):
        runner = _fake_runner(writes="x")
        fetch_log("20260731-181013-cfd2e8", "eci3", root=self.root, runner=runner)
        argv = runner.calls[0]
        self.assertIn("eci3:/home/ubuntu/manager_jobs/20260731-181013-cfd2e8.log", argv)
        self.assertIn("BatchMode=yes", argv)

    def test_absent_remote_log_is_reported_as_missing_not_error(self):
        runner = _fake_runner(returncode=1, stderr="scp: /home/ubuntu/manager_jobs/J1.log: No such file or directory")
        result = fetch_log("J1", "eci7", root=self.root, runner=runner)
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "missing")

    def test_unreachable_host_is_an_error_not_missing(self):
        """Errors are retried; `missing` is remembered. Confusing them is costly."""
        runner = _fake_runner(returncode=255, stderr="ssh: Could not resolve hostname eci7")
        result = fetch_log("J1", "eci7", root=self.root, runner=runner)
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "error")
        self.assertIn("Could not resolve", result["detail"])

    def test_timeout_is_an_error(self):
        runner = _fake_runner(returncode=124, stderr="scp timeout")
        self.assertEqual(fetch_log("J1", "eci7", root=self.root, runner=runner)["reason"], "error")


class PruneAndForgetTests(_LogFixture):
    def _write(self, name, size, mtime):
        import os
        self.root.mkdir(parents=True, exist_ok=True)
        path = self.root / name
        path.write_bytes(b"x" * size)
        os.utime(path, (mtime, mtime))
        return path

    def test_under_budget_keeps_everything(self):
        self._write("a.log", 100, 1000)
        self.assertEqual(prune_log_cache(max_bytes=1000, root=self.root), 0)

    def test_oldest_are_dropped_until_under_budget(self):
        old = self._write("old.log", 500, 1000)
        mid = self._write("mid.log", 500, 2000)
        new = self._write("new.log", 500, 3000)
        removed = prune_log_cache(max_bytes=1000, root=self.root)
        self.assertEqual(removed, 1)
        self.assertFalse(old.exists())
        self.assertTrue(mid.exists())
        self.assertTrue(new.exists())

    def test_missing_directory_is_not_an_error(self):
        self.assertEqual(prune_log_cache(max_bytes=10, root=self.root / "nope"), 0)

    def test_forget_removes_cached_logs(self):
        self._write("J1.log", 10, 1000)
        self.assertEqual(forget_logs(["J1", "NEVER-FETCHED"], root=self.root), 1)
        self.assertFalse((self.root / "J1.log").exists())


if __name__ == "__main__":
    unittest.main()
