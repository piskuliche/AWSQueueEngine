import tempfile
import unittest
from pathlib import Path

from awsqueueengine.shared import queue


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

    def test_dequeue_for_host_prefers_resume_first_for_that_host(self):
        queue.save_queue([
            {"cmd": "high-priority-generic", "priority": 1000},
            {
                "cmd": "resume-eci18",
                "priority": -10,
                "hosts": ["eci18"],
                "resume_first": True,
                "resume_host": "eci18",
            },
            {"cmd": "regular-eci18", "priority": 50, "hosts": ["eci18"]},
        ])

        first = queue.dequeue_for_host("eci18")
        second = queue.dequeue_for_host("eci18")

        self.assertEqual(first["cmd"], "resume-eci18")
        self.assertEqual(second["cmd"], "high-priority-generic")

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
        self.assertFalse(first["preempt"])
        self.assertIsNone(first["payload_remote_path"])
        self.assertIsNone(first["payload_s3_uri"])
        self.assertIsNone(first["payload_size_bytes"])
        self.assertFalse(first["resume_first"])
        self.assertIsNone(first["resume_host"])

        self.assertEqual(second["cmd"], "echo old-style")
        self.assertEqual(second["priority"], 0)
        self.assertIsNone(second["hosts"])
        self.assertFalse(second["preempt"])
        self.assertIsNone(second["payload_remote_path"])
        self.assertIsNone(second["payload_s3_uri"])
        self.assertIsNone(second["payload_size_bytes"])
        self.assertFalse(second["resume_first"])
        self.assertIsNone(second["resume_host"])

        self.assertEqual(third["cmd"], "legacy-normal")
        self.assertEqual(third["priority"], 0)
        self.assertIsNone(third["hosts"])
        self.assertFalse(third["preempt"])
        self.assertIsNone(third["payload_remote_path"])
        self.assertIsNone(third["payload_s3_uri"])
        self.assertIsNone(third["payload_size_bytes"])
        self.assertFalse(third["resume_first"])
        self.assertIsNone(third["resume_host"])

        self.assertEqual(fourth["cmd"], "legacy-default")
        self.assertEqual(fourth["payload"], "/tmp/x")
        self.assertEqual(fourth["priority"], 0)
        self.assertIsNone(fourth["hosts"])
        self.assertFalse(fourth["preempt"])
        self.assertIsNone(fourth["payload_remote_path"])
        self.assertIsNone(fourth["payload_s3_uri"])
        self.assertIsNone(fourth["payload_size_bytes"])
        self.assertFalse(fourth["resume_first"])
        self.assertIsNone(fourth["resume_host"])

    def test_enqueue_item_persists_canonical_fields(self):
        queue.enqueue_item({"cmd": "job", "priority": "high", "hosts": "eci17"})

        stored = queue.load_queue()
        self.assertEqual(len(stored), 1)
        self.assertEqual(
            stored[0],
            {
                "cmd": "job",
                "payload": None,
                "priority": 100,
                "queue": "default",
                "hosts": ["eci17"],
                "preempt": False,
                "mps": False,
                "payload_remote_path": None,
                "payload_s3_uri": None,
                "payload_size_bytes": None,
                "resume_first": False,
                "resume_host": None,
                "job_id": None,
                "submit_failures": 0,
            },
        )

    def test_build_resume_item_pins_host_and_can_override_priority(self):
        resume_item = queue.build_resume_item(
            {
                "cmd": "python train.py",
                "payload": "/tmp/local",
                "payload_remote_path": "/remote/payload",
                "payload_s3_uri": "s3://bucket/key.tar.gz",
                "payload_size_bytes": 123,
                "priority": 3,
                "hosts": ["eci2", "eci3"],
                "preempt": True,
                "submit_failures": 2,
            },
            "eci7",
            priority=100,
        )

        self.assertEqual(resume_item["cmd"], "python train.py")
        self.assertEqual(resume_item["payload"], "/tmp/local")
        self.assertEqual(resume_item["payload_remote_path"], "/remote/payload")
        self.assertEqual(resume_item["payload_s3_uri"], "s3://bucket/key.tar.gz")
        self.assertEqual(resume_item["payload_size_bytes"], 123)
        self.assertEqual(resume_item["priority"], 100)
        self.assertEqual(resume_item["hosts"], ["eci7"])
        self.assertTrue(resume_item["preempt"])
        self.assertTrue(resume_item["resume_first"])
        self.assertEqual(resume_item["resume_host"], "eci7")
        self.assertEqual(resume_item["submit_failures"], 0)


if __name__ == "__main__":
    unittest.main()
