import unittest

from awsqueueengine.monitor import _prune_running_jobs_for_status


class MonitorRunningStatePruneTests(unittest.TestCase):
    def test_unreachable_host_keeps_running_metadata(self):
        running_jobs = {
            "eci5": {"cmd": "run-a"},
            "eci6": {"cmd": "run-b"},
        }
        status_rows = [
            {"host": "eci5", "reachable": False, "pid": None},
            {"host": "eci6", "reachable": True, "pid": "123"},
        ]

        changed = _prune_running_jobs_for_status(running_jobs, status_rows)

        self.assertFalse(changed)
        self.assertEqual(set(running_jobs), {"eci5", "eci6"})

    def test_reachable_idle_host_is_pruned(self):
        running_jobs = {
            "eci5": {"cmd": "run-a"},
            "eci6": {"cmd": "run-b"},
        }
        status_rows = [
            {"host": "eci5", "reachable": True, "pid": None},
            {"host": "eci6", "reachable": True, "pid": "123"},
        ]

        changed = _prune_running_jobs_for_status(running_jobs, status_rows)

        self.assertTrue(changed)
        self.assertEqual(set(running_jobs), {"eci6"})


if __name__ == "__main__":
    unittest.main()
