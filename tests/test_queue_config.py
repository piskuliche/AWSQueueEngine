import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from awsqueueengine.shared.queue_config import QueueConfigSource, host_is_eligible_for_item


class QueueConfigTests(unittest.TestCase):
    def test_file_config_reloads_queue_hosts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            queues_file = Path(tmpdir) / "queues.json"
            queues_file.write_text(json.dumps({"default": ["eci1"], "fast": ["eci2"]}))

            with patch.dict(
                "os.environ",
                {"AWSQUEUEENGINE_QUEUES_FILE": str(queues_file), "AWSQUEUEENGINE_QUEUES": ""},
                clear=True,
            ):
                source = QueueConfigSource()
                self.assertEqual(source.refresh(), {"default": ["eci1"], "fast": ["eci2"]})

                queues_file.write_text(json.dumps({"default": ["eci1"], "fast": ["eci3"]}))
                self.assertEqual(source.refresh(), {"default": ["eci1"], "fast": ["eci3"]})

    def test_env_and_file_are_mutually_exclusive(self):
        with patch.dict(
            "os.environ",
            {
                "AWSQUEUEENGINE_QUEUES_FILE": "/tmp/queues.json",
                "AWSQUEUEENGINE_QUEUES": "default=eci1",
            },
            clear=True,
        ):
            with self.assertRaises(ValueError):
                QueueConfigSource()

    def test_queue_eligibility_uses_queue_name_when_no_legacy_hosts(self):
        queue_map = {"default": ["eci1"], "fast": ["eci2"]}

        self.assertTrue(host_is_eligible_for_item({"queue": "fast", "hosts": None}, "eci2", queue_map))
        self.assertFalse(host_is_eligible_for_item({"queue": "fast", "hosts": None}, "eci1", queue_map))

    def test_legacy_hosts_still_override_queue_for_resume_records(self):
        queue_map = {"default": ["eci1"], "fast": ["eci2"]}

        self.assertTrue(host_is_eligible_for_item({"queue": "fast", "hosts": ["eci1"]}, "eci1", queue_map))
        self.assertFalse(host_is_eligible_for_item({"queue": "fast", "hosts": ["eci1"]}, "eci2", queue_map))


if __name__ == "__main__":
    unittest.main()
