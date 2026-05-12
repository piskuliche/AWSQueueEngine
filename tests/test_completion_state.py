import tempfile
import unittest
from pathlib import Path

from awsqueueengine.shared import completion_state


class CompletionStateTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.completed_file = Path(self.tmpdir.name) / "completed.json"
        self.original_completed_file = completion_state.COMPLETED_FILE
        completion_state.COMPLETED_FILE = self.completed_file

    def tearDown(self):
        completion_state.COMPLETED_FILE = self.original_completed_file
        self.tmpdir.cleanup()

    def test_load_returns_empty_for_missing_or_invalid_file(self):
        self.assertEqual(completion_state.load_completed_jobs(), [])

        self.completed_file.write_text("not-json")
        self.assertEqual(completion_state.load_completed_jobs(), [])

        self.completed_file.write_text("{}")
        self.assertEqual(completion_state.load_completed_jobs(), [])

    def test_append_completed_records_appends_history(self):
        completion_state.append_completed_records([{"host": "eci1", "dur": "00:01:00"}])
        completion_state.append_completed_records([{"host": "eci2", "dur": "00:02:00"}])
        loaded = completion_state.load_completed_jobs()
        self.assertEqual(len(loaded), 2)
        self.assertEqual(loaded[0]["host"], "eci1")
        self.assertEqual(loaded[1]["host"], "eci2")


if __name__ == "__main__":
    unittest.main()
