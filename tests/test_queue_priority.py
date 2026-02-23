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

    def test_dequeue_picks_highest_integer_priority_first(self):
        queue.save_queue([
            {"cmd": "p0", "payload": None, "priority": 0},
            {"cmd": "p200", "payload": None, "priority": 200},
            {"cmd": "p100", "payload": None, "priority": 100},
            {"cmd": "p-1", "payload": None, "priority": -1},
        ])

        first = queue.dequeue()
        second = queue.dequeue()
        third = queue.dequeue()
        fourth = queue.dequeue()

        self.assertEqual(first["cmd"], "p200")
        self.assertEqual(second["cmd"], "p100")
        self.assertEqual(third["cmd"], "p0")
        self.assertEqual(fourth["cmd"], "p-1")
        self.assertIsNone(queue.dequeue())

    def test_dequeue_equal_priority_is_fifo(self):
        queue.save_queue([
            {"cmd": "job-1", "payload": None, "priority": 10},
            {"cmd": "job-2", "payload": None, "priority": 10},
            {"cmd": "job-3", "payload": None, "priority": 10},
        ])

        self.assertEqual(queue.dequeue()["cmd"], "job-1")
        self.assertEqual(queue.dequeue()["cmd"], "job-2")
        self.assertEqual(queue.dequeue()["cmd"], "job-3")

    def test_dequeue_for_host_skips_ineligible_items(self):
        queue.save_queue([
            {"cmd": "eci17-only", "payload": None, "priority": 100, "hosts": ["eci17"]},
            {"cmd": "any-host", "payload": None, "priority": 5},
            {"cmd": "eci18-only", "payload": None, "priority": 20, "hosts": ["eci18"]},
        ])

        first = queue.dequeue_for_host("eci18")
        second = queue.dequeue_for_host("eci19")
        third = queue.dequeue_for_host("eci17")

        self.assertEqual(first["cmd"], "eci18-only")
        self.assertEqual(second["cmd"], "any-host")
        self.assertEqual(third["cmd"], "eci17-only")
        self.assertIsNone(queue.dequeue())

    def test_legacy_items_are_normalized(self):
        queue.save_queue([
            "echo old-style",
            {"cmd": "legacy-high", "payload": None, "priority": "high"},
            {"cmd": "legacy-normal", "payload": None, "priority": "normal"},
            {"cmd": "legacy-default", "payload": "/tmp/x"},
        ])

        first = queue.dequeue()
        second = queue.dequeue()
        third = queue.dequeue()
        fourth = queue.dequeue()

        self.assertEqual(first["cmd"], "legacy-high")
        self.assertEqual(first["priority"], 100)
        self.assertIsNone(first["hosts"])

        self.assertEqual(second["cmd"], "echo old-style")
        self.assertEqual(second["priority"], 0)
        self.assertIsNone(second["hosts"])

        self.assertEqual(third["cmd"], "legacy-normal")
        self.assertEqual(third["priority"], 0)
        self.assertIsNone(third["hosts"])

        self.assertEqual(fourth["cmd"], "legacy-default")
        self.assertEqual(fourth["payload"], "/tmp/x")
        self.assertEqual(fourth["priority"], 0)
        self.assertIsNone(fourth["hosts"])

    def test_enqueue_item_persists_canonical_fields(self):
        queue.enqueue_item({"cmd": "job", "priority": "high", "hosts": "eci17"})

        stored = queue.load_queue()
        self.assertEqual(len(stored), 1)
        self.assertEqual(
            stored[0],
            {"cmd": "job", "payload": None, "priority": 100, "hosts": ["eci17"]},
        )


if __name__ == "__main__":
    unittest.main()
