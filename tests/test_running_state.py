import tempfile
import unittest
from pathlib import Path

from awsqueueengine import running_state


class RunningStateTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.running_file = Path(self.tmpdir.name) / "running.json"
        self.original_running_file = running_state.RUNNING_FILE
        running_state.RUNNING_FILE = self.running_file

    def tearDown(self):
        running_state.RUNNING_FILE = self.original_running_file
        self.tmpdir.cleanup()

    def test_load_returns_empty_for_missing_or_invalid_file(self):
        self.assertEqual(running_state.load_running_jobs(), {})

        self.running_file.write_text("not-json")
        self.assertEqual(running_state.load_running_jobs(), {})

        self.running_file.write_text("[]")
        self.assertEqual(running_state.load_running_jobs(), {})

    def test_save_and_load_normalizes_payload(self):
        running_state.save_running_jobs(
            {
                "eci1": {"cmd": "echo hi", "priority": "100", "preempt": "yes", "started_at": "123.5"},
                " ": {"cmd": "ignored"},
                123: {"cmd": "ignored-too"},
            }
        )

        loaded = running_state.load_running_jobs()
        self.assertEqual(set(loaded.keys()), {"eci1"})
        self.assertEqual(loaded["eci1"]["cmd"], "echo hi")
        self.assertEqual(loaded["eci1"]["priority"], 100)
        self.assertTrue(loaded["eci1"]["preempt"])
        self.assertIn("payload_remote_path", loaded["eci1"])
        self.assertIn("payload_s3_uri", loaded["eci1"])
        self.assertIn("payload_size_bytes", loaded["eci1"])
        self.assertIn("resume_first", loaded["eci1"])
        self.assertIn("resume_host", loaded["eci1"])
        self.assertIn("started_at", loaded["eci1"])
        self.assertEqual(loaded["eci1"]["started_at"], 123.5)


if __name__ == "__main__":
    unittest.main()
