import tempfile
import unittest
from pathlib import Path

from awsqueueengine.shared import failure_state


class FailureStateTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.failed_file = Path(self.tmpdir.name) / "failed.json"
        self.original_failed_file = failure_state.FAILED_FILE
        failure_state.FAILED_FILE = self.failed_file

    def tearDown(self):
        failure_state.FAILED_FILE = self.original_failed_file
        self.tmpdir.cleanup()

    def test_load_returns_empty_for_missing_or_invalid_file(self):
        self.assertEqual(failure_state.load_failed_jobs(), [])

        self.failed_file.write_text("not-json")
        self.assertEqual(failure_state.load_failed_jobs(), [])

        self.failed_file.write_text("{}")
        self.assertEqual(failure_state.load_failed_jobs(), [])

    def test_append_failed_records_appends_history(self):
        failure_state.append_failed_records([{"job_id": "A", "failure_reason": "segfault"}])
        failure_state.append_failed_records([{"job_id": "B", "failure_reason": "out_of_memory"}])
        loaded = failure_state.load_failed_jobs()
        self.assertEqual([r["job_id"] for r in loaded], ["A", "B"])

    def test_append_empty_records_is_a_noop(self):
        failure_state.append_failed_records([])
        self.assertFalse(self.failed_file.exists())

    def test_history_is_capped_keeping_the_newest(self):
        cap = failure_state.MAX_FAILED_RECORDS
        failure_state.append_failed_records([{"job_id": str(i)} for i in range(cap + 5)])
        loaded = failure_state.load_failed_jobs()
        self.assertEqual(len(loaded), cap)
        self.assertEqual(loaded[0]["job_id"], "5")
        self.assertEqual(loaded[-1]["job_id"], str(cap + 4))


if __name__ == "__main__":
    unittest.main()
