import tempfile
import unittest
from pathlib import Path

from awsqueueengine import queue


class QueuePriorityTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.queue_file = Path(self.tmpdir.name) / "queue.json"
        self.original_queue_file = queue.QUEUE_FILE
        queue.QUEUE_FILE = self.queue_file

    def tearDown(self):
        queue.QUEUE_FILE = self.original_queue_file
        self.tmpdir.cleanup()

    def test_dequeue_picks_high_priority_first(self):
        queue.save_queue([
            {"cmd": "normal-1", "payload": None, "priority": "normal"},
            {"cmd": "high-1", "payload": None, "priority": "high"},
            {"cmd": "normal-2", "payload": None, "priority": "normal"},
            {"cmd": "high-2", "payload": None, "priority": "high"},
        ])

        first = queue.dequeue()
        second = queue.dequeue()
        third = queue.dequeue()
        fourth = queue.dequeue()

        self.assertEqual(first["cmd"], "high-1")
        self.assertEqual(second["cmd"], "high-2")
        self.assertEqual(third["cmd"], "normal-1")
        self.assertEqual(fourth["cmd"], "normal-2")
        self.assertIsNone(queue.dequeue())

    def test_dequeue_legacy_string_item_defaults_to_normal_priority(self):
        queue.save_queue(["echo old-style"])

        item = queue.dequeue()

        self.assertEqual(
            item,
            {"cmd": "echo old-style", "payload": None, "priority": "normal"},
        )

    def test_dequeue_legacy_dict_item_defaults_to_normal_priority(self):
        queue.save_queue([{"cmd": "echo old-dict", "payload": "/tmp/x"}])

        item = queue.dequeue()

        self.assertEqual(item["cmd"], "echo old-dict")
        self.assertEqual(item["payload"], "/tmp/x")
        self.assertEqual(item["priority"], "normal")


if __name__ == "__main__":
    unittest.main()
