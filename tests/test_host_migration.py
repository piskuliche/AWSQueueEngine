"""Tests for awsqueueengine.host.migration."""
import json
import tempfile
import unittest
from pathlib import Path

from awsqueueengine.host import migration
from awsqueueengine.shared import paths as paths_mod


class _MigrationFixture(unittest.TestCase):
    """Redirect both the legacy ``$HOME`` source and the destination
    ``~/.awsqe/host/`` paths into a single tempdir so the test can stage and
    inspect files without touching the real $HOME."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

        # Redirect the destination paths in shared.paths so the migrate()
        # callers (and the constants imported into other modules) all land in
        # the tempdir. We restore on tearDown.
        self._orig_paths = {}
        for name, rel in [
            ("HOST_STATE_DIR", Path(".awsqe") / "host"),
            ("QUEUE_FILE",     Path(".awsqe") / "host" / "queue.json"),
            ("RUNNING_FILE",   Path(".awsqe") / "host" / "running.json"),
            ("COMPLETED_FILE", Path(".awsqe") / "host" / "completed.json"),
            ("DEFERRED_FILE",  Path(".awsqe") / "host" / "deferred.json"),
            ("MONITOR_STATE_FILE", Path(".awsqe") / "host" / "monitor_state.json"),
            ("LOCK_FILE",      Path(".awsqe") / "host" / "lock"),
            ("PIDFILE",        Path(".awsqe") / "host" / "pid"),
        ]:
            self._orig_paths[name] = getattr(paths_mod, name)
            setattr(paths_mod, name, self.tmp / rel)
        # migration.py imported the path constants at module-load time, so it
        # has its own bindings we have to rebind too.
        for name in ("QUEUE_FILE", "RUNNING_FILE", "COMPLETED_FILE",
                     "DEFERRED_FILE", "MONITOR_STATE_FILE", "HOST_STATE_DIR"):
            setattr(migration, name, getattr(paths_mod, name))

    def tearDown(self):
        for name, original in self._orig_paths.items():
            setattr(paths_mod, name, original)
        for name in ("QUEUE_FILE", "RUNNING_FILE", "COMPLETED_FILE",
                     "DEFERRED_FILE", "MONITOR_STATE_FILE", "HOST_STATE_DIR"):
            setattr(migration, name, getattr(paths_mod, name))
        self._tmp.cleanup()

    def _stage_legacy(self, name, payload):
        """Write a legacy file into the tempdir's "home" so migration sees it."""
        legacy = self.tmp / f".aws_slurm_like_{name}.json"
        legacy.write_text(json.dumps(payload, indent=2))
        return legacy

    def _migrate(self, **kwargs):
        # Always pass home=self.tmp so the legacy source resolves under the fixture.
        return migration.migrate(home=self.tmp, **kwargs)


class FreshInstallTests(_MigrationFixture):
    def test_no_legacy_files_marks_migrated_and_moves_nothing(self):
        result = self._migrate()
        self.assertEqual(result.moved, [])
        self.assertEqual(len(result.skipped_legacy_absent), 5)
        self.assertEqual(result.backups, [])
        # monitor_state.json was created just to record migrated_at.
        self.assertTrue(migration.is_migrated(paths_mod.MONITOR_STATE_FILE))


class LegacyPresentTests(_MigrationFixture):
    def test_legacy_files_get_copied_and_renamed_to_bak(self):
        self._stage_legacy("queue", [{"cmd": "echo hi", "job_id": "X"}])
        self._stage_legacy("running", {"eci1": {"cmd": "live"}})
        self._stage_legacy("completed", [{"job_id": "Y", "cmd": "done"}])
        self._stage_legacy("deferred", [])

        result = self._migrate()

        self.assertEqual(len(result.moved), 4)
        # All four new files exist with the same JSON content.
        self.assertEqual(json.loads(paths_mod.QUEUE_FILE.read_text())[0]["job_id"], "X")
        self.assertEqual(json.loads(paths_mod.RUNNING_FILE.read_text())["eci1"]["cmd"], "live")
        self.assertEqual(json.loads(paths_mod.COMPLETED_FILE.read_text())[0]["job_id"], "Y")
        self.assertEqual(json.loads(paths_mod.DEFERRED_FILE.read_text()), [])
        # Legacy files are gone, .migrated.bak files exist.
        for name in ("queue", "running", "completed", "deferred"):
            self.assertFalse((self.tmp / f".aws_slurm_like_{name}.json").exists())
            self.assertTrue((self.tmp / f".aws_slurm_like_{name}.json.migrated.bak").exists())
        # is_migrated() returns True after the run.
        self.assertTrue(migration.is_migrated(paths_mod.MONITOR_STATE_FILE))


class IdempotencyTests(_MigrationFixture):
    def test_second_run_is_a_noop(self):
        self._stage_legacy("queue", [{"cmd": "echo hi", "job_id": "X"}])
        first = self._migrate()
        self.assertEqual(len(first.moved), 1)

        second = self._migrate()
        self.assertTrue(second.already_migrated)
        self.assertEqual(second.moved, [])

    def test_force_re_runs_even_when_already_migrated(self):
        self._stage_legacy("queue", [{"cmd": "first", "job_id": "X"}])
        self._migrate()
        # Stage a new legacy file that wasn't there before; --force picks it up.
        self._stage_legacy("running", {"eci1": {"cmd": "live"}})
        result = self._migrate(force=True)
        self.assertEqual(len(result.moved), 1)
        # The legacy `queue` legacy file is now `.migrated.bak`, and the new
        # one exists; force re-runs but doesn't clobber.
        self.assertTrue(paths_mod.RUNNING_FILE.exists())


class ExistingDestinationTests(_MigrationFixture):
    def test_skip_when_new_file_already_exists(self):
        # Pre-populate the new location.
        paths_mod.QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
        paths_mod.QUEUE_FILE.write_text(json.dumps([{"cmd": "already-here"}]))
        # Stage a legacy file too.
        self._stage_legacy("queue", [{"cmd": "would-overwrite"}])

        result = self._migrate()
        self.assertEqual(len(result.skipped_new_exists), 1)
        self.assertEqual(result.moved, [])
        # The new file is untouched (no clobber).
        self.assertEqual(
            json.loads(paths_mod.QUEUE_FILE.read_text())[0]["cmd"],
            "already-here",
        )
        # The legacy file is left in place (no rename).
        self.assertTrue((self.tmp / ".aws_slurm_like_queue.json").exists())

    def test_force_overwrites_existing_destination(self):
        paths_mod.QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
        paths_mod.QUEUE_FILE.write_text(json.dumps([{"cmd": "stale"}]))
        self._stage_legacy("queue", [{"cmd": "fresh"}])

        self._migrate(force=True)
        self.assertEqual(
            json.loads(paths_mod.QUEUE_FILE.read_text())[0]["cmd"],
            "fresh",
        )


class DryRunTests(_MigrationFixture):
    def test_dry_run_writes_nothing(self):
        self._stage_legacy("queue", [{"cmd": "echo hi", "job_id": "X"}])
        result = self._migrate(dry_run=True)
        self.assertEqual(len(result.moved), 1)
        # Critically: nothing actually moved on disk.
        self.assertTrue((self.tmp / ".aws_slurm_like_queue.json").exists())
        self.assertFalse(paths_mod.QUEUE_FILE.exists())
        self.assertFalse((self.tmp / ".aws_slurm_like_queue.json.migrated.bak").exists())
        # And the migrated_at stamp wasn't written (so a real run would still do work).
        self.assertFalse(migration.is_migrated(paths_mod.MONITOR_STATE_FILE))


class AutoMigrateTests(_MigrationFixture):
    def test_auto_migrate_runs_when_not_yet_migrated(self):
        self._stage_legacy("queue", [{"cmd": "auto", "job_id": "Z"}])
        result = migration.auto_migrate_if_needed(home=self.tmp)
        self.assertIsNotNone(result)
        self.assertEqual(len(result.moved), 1)

    def test_auto_migrate_is_silent_once_migrated(self):
        migration.migrate(home=self.tmp)  # establishes migrated_at
        self.assertIsNone(migration.auto_migrate_if_needed(home=self.tmp))


class RenderSummaryTests(_MigrationFixture):
    def test_summary_already_migrated(self):
        migration.migrate(home=self.tmp)
        result = self._migrate()
        text = migration.render_summary(result, dry_run=False)
        self.assertIn("Already migrated", text)
        self.assertIn("--force", text)

    def test_summary_no_legacy_files(self):
        result = self._migrate()
        text = migration.render_summary(result, dry_run=False)
        self.assertIn("No legacy state files found", text)

    def test_summary_dry_run_uses_would_move_wording(self):
        self._stage_legacy("queue", [{"cmd": "echo"}])
        result = self._migrate(dry_run=True)
        text = migration.render_summary(result, dry_run=True)
        self.assertIn("would move", text)
        self.assertIn("would rename", text)


if __name__ == "__main__":
    unittest.main()
