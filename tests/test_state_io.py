import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

from awsqueueengine.shared import state_io


class UnserializableOnPurpose:
    """json.dumps raises TypeError on this; nothing else about it matters."""


class WriteJsonAtomicTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self.target = self.root / "nested" / "state.json"

    def tearDown(self):
        self.tmpdir.cleanup()

    def _litter(self):
        """Any scratch files left next to the target."""
        parent = self.target.parent
        if not parent.exists():
            return []
        return sorted(p.name for p in parent.iterdir() if ".tmp." in p.name)

    def test_creates_parent_directory(self):
        self.assertFalse(self.target.parent.exists())
        state_io.write_json_atomic(self.target, [1, 2, 3])
        self.assertEqual(json.loads(self.target.read_text()), [1, 2, 3])

    def test_returns_the_path_written(self):
        self.assertEqual(state_io.write_json_atomic(self.target, {}), self.target)

    def test_leaves_no_scratch_file_behind(self):
        state_io.write_json_atomic(self.target, {"a": 1})
        self.assertEqual(self._litter(), [])

    def test_scratch_path_is_pid_scoped(self):
        # A shared `.tmp` would let a scratch file abandoned by a killed process
        # become the source of somebody else's replace.
        tmp = state_io.temp_path_for(self.target)
        self.assertTrue(tmp.name.endswith(f".tmp.{os.getpid()}"))

    def test_replaces_existing_content_wholesale(self):
        state_io.write_json_atomic(self.target, [{"job_id": "old"}])
        state_io.write_json_atomic(self.target, [{"job_id": "new"}])
        self.assertEqual(json.loads(self.target.read_text()), [{"job_id": "new"}])

    def test_failed_write_preserves_previous_content(self):
        state_io.write_json_atomic(self.target, {"keep": "me"})

        with self.assertRaises(TypeError):
            state_io.write_json_atomic(self.target, {"bad": UnserializableOnPurpose()})

        self.assertEqual(json.loads(self.target.read_text()), {"keep": "me"})
        self.assertEqual(self._litter(), [])

    def test_failed_write_to_a_new_path_creates_nothing(self):
        with self.assertRaises(TypeError):
            state_io.write_json_atomic(self.target, UnserializableOnPurpose())

        self.assertFalse(self.target.exists())
        self.assertEqual(self._litter(), [])

    def test_failed_replace_cleans_up_the_scratch_file(self):
        # A target that is a directory makes os.replace fail *after* the scratch
        # file exists — the one path where cleanup actually has work to do.
        blocked = self.root / "a-directory"
        blocked.mkdir()
        with self.assertRaises(OSError):
            state_io.write_json_atomic(blocked, [1])
        self.assertEqual(sorted(p.name for p in self.root.iterdir() if ".tmp." in p.name), [])


class WriteTextAtomicTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.target = Path(self.tmpdir.name) / "client.toml"

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_writes_text_verbatim(self):
        state_io.write_text_atomic(self.target, "[default]\nqueue = \"gpu\"\n")
        self.assertEqual(self.target.read_text(), "[default]\nqueue = \"gpu\"\n")


class WarnUnreadableTests(unittest.TestCase):
    def test_names_the_file_and_the_error_on_stderr(self):
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            state_io.warn_unreadable(Path("/tmp/queue.json"), ValueError("boom"))
        message = stderr.getvalue()
        self.assertIn("/tmp/queue.json", message)
        self.assertIn("boom", message)


if __name__ == "__main__":
    unittest.main()
