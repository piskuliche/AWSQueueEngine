import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from awsqueueengine.shared import queue_config as queue_config_mod
from awsqueueengine.shared.queue_config import (
    QueueConfigSource,
    get_configured_queue_source,
    host_is_eligible_for_item,
)


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


class DefaultPathDiscoveryTests(unittest.TestCase):
    """The host daemon and `awsqe-host rpc` subprocesses agree on the queue config
    without depending on env-var inheritance through non-interactive SSH.
    """

    def _clear_env(self):
        return patch.dict("os.environ",
                          {"AWSQUEUEENGINE_QUEUES_FILE": "", "AWSQUEUEENGINE_QUEUES": ""},
                          clear=True)

    def test_no_env_no_files_returns_none_none(self):
        with self._clear_env(), patch.object(queue_config_mod, "DEFAULT_QUEUES_CONFIG_PATHS", ()):
            self.assertEqual(get_configured_queue_source(), (None, None))

    def test_canonical_path_used_when_present(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            canonical = Path(tmpdir) / "canonical.json"
            canonical.write_text(json.dumps({"default": ["eci1"]}))
            with self._clear_env(), patch.object(
                queue_config_mod, "DEFAULT_QUEUES_CONFIG_PATHS", (canonical,)
            ):
                kind, value = get_configured_queue_source()
                self.assertEqual(kind, "file")
                self.assertEqual(value, str(canonical))

    def test_canonical_wins_over_legacy_when_both_present(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            canonical = Path(tmpdir) / "canonical.json"
            legacy = Path(tmpdir) / "legacy.json"
            canonical.write_text(json.dumps({"default": ["eci1"]}))
            legacy.write_text(json.dumps({"default": ["eci99"]}))
            with self._clear_env(), patch.object(
                queue_config_mod, "DEFAULT_QUEUES_CONFIG_PATHS", (canonical, legacy)
            ):
                kind, value = get_configured_queue_source()
                self.assertEqual(value, str(canonical))

    def test_legacy_path_used_when_canonical_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            canonical = Path(tmpdir) / "canonical.json"  # not created
            legacy = Path(tmpdir) / "legacy.json"
            legacy.write_text(json.dumps({"default": ["eci99"]}))
            with self._clear_env(), patch.object(
                queue_config_mod, "DEFAULT_QUEUES_CONFIG_PATHS", (canonical, legacy)
            ):
                kind, value = get_configured_queue_source()
                self.assertEqual(value, str(legacy))

    def test_env_var_wins_over_default_paths(self):
        # If the operator pointed at a specific file, don't second-guess them
        # by also reading a stray canonical file that happens to exist.
        with tempfile.TemporaryDirectory() as tmpdir:
            canonical = Path(tmpdir) / "canonical.json"
            canonical.write_text(json.dumps({"default": ["eci1"]}))
            chosen = Path(tmpdir) / "chosen.json"
            chosen.write_text(json.dumps({"default": ["eciX"]}))
            with patch.dict(
                "os.environ",
                {"AWSQUEUEENGINE_QUEUES_FILE": str(chosen), "AWSQUEUEENGINE_QUEUES": ""},
                clear=True,
            ), patch.object(queue_config_mod, "DEFAULT_QUEUES_CONFIG_PATHS", (canonical,)):
                kind, value = get_configured_queue_source()
                self.assertEqual(value, str(chosen))

    def test_queue_config_source_constructs_from_discovered_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            canonical = Path(tmpdir) / "canonical.json"
            canonical.write_text(json.dumps({"default": ["eci1"], "fast": ["eci2"]}))
            with self._clear_env(), patch.object(
                queue_config_mod, "DEFAULT_QUEUES_CONFIG_PATHS", (canonical,)
            ):
                source = QueueConfigSource()
                self.assertEqual(source.refresh(), {"default": ["eci1"], "fast": ["eci2"]})
                self.assertEqual(source.source_kind, "file")


if __name__ == "__main__":
    unittest.main()
