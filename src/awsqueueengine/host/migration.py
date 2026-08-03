"""One-shot migration from `~/.aws_slurm_like_*.json` → `~/.awsqe/host/*.json`.

Phase 5 moves the queue host's state files into a single directory. Existing
queue hosts have the legacy files in `$HOME`; this module copies each one to
the new location, renames the legacy file to `*.migrated.bak` (recovery
hatch — operator can roll back by hand by moving the .bak back), and writes
a ``migrated_at`` timestamp into the new ``monitor_state.json`` so we never
re-run the migration once it's done.

Idempotent: once ``migrated_at`` is present, ``migrate()`` is a no-op
unless ``--force`` is passed (for recovery). Auto-runs at the top of
``awsqe-host monitor`` so a daemon restart after upgrade picks up the
operator's existing queue.
"""
from __future__ import annotations

import json
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

from ..shared.paths import (
    COMPLETED_FILE,
    DEFERRED_FILE,
    HOST_STATE_DIR,
    MONITOR_STATE_FILE,
    QUEUE_FILE,
    RUNNING_FILE,
)
from ..shared.state_io import write_json_atomic


# Each entry: (legacy path, new path).
def _migration_pairs(home: Path | None = None) -> list[tuple[Path, Path]]:
    """The legacy → new path pairs to migrate.

    Accepts an explicit ``home`` so tests can redirect to a tempdir without
    monkey-patching every constant.
    """
    legacy_home = home if home is not None else Path.home()
    return [
        (legacy_home / ".aws_slurm_like_queue.json",           QUEUE_FILE),
        (legacy_home / ".aws_slurm_like_running.json",         RUNNING_FILE),
        (legacy_home / ".aws_slurm_like_completed.json",       COMPLETED_FILE),
        (legacy_home / ".aws_slurm_like_deferred.json",        DEFERRED_FILE),
        (legacy_home / ".aws_slurm_like_monitor_state.json",   MONITOR_STATE_FILE),
    ]


@dataclass
class MigrationResult:
    moved: list[tuple[Path, Path]]   # legacy → new pairs we actually moved
    skipped_legacy_absent: list[Path]   # legacy file didn't exist
    skipped_new_exists: list[Path]   # new file already there (and not --force)
    backups: list[Path]   # .migrated.bak files we created
    already_migrated: bool = False  # short-circuited because migrated_at was set


# ---------- public API ----------

def is_migrated(state_file: Path = MONITOR_STATE_FILE) -> bool:
    """True iff the new monitor_state.json has a ``migrated_at`` timestamp."""
    if not state_file.exists():
        return False
    try:
        data = json.loads(state_file.read_text())
    except Exception:
        return False
    return isinstance(data, dict) and "migrated_at" in data


def migrate(
    *,
    dry_run: bool = False,
    force: bool = False,
    home: Path | None = None,
) -> MigrationResult:
    """Migrate legacy state files into ``~/.awsqe/host/``.

    Idempotent: if the new monitor_state.json already has ``migrated_at`` set,
    return early with ``already_migrated=True`` unless ``force=True``.

    Each legacy file:
      * If the new file already exists (and not ``--force``): skip; record in
        ``skipped_new_exists``. We do NOT clobber existing new state.
      * If the legacy file doesn't exist: skip; record in
        ``skipped_legacy_absent``.
      * Otherwise: copy legacy → new (preserving mtime/perms via copy2), then
        rename legacy to ``legacy.migrated.bak``.

    ``home`` is an injection seam for tests; in production it's ``Path.home()``.
    """
    pairs = _migration_pairs(home)
    new_state_file = pairs[-1][1]  # MONITOR_STATE_FILE for this `home`

    result = MigrationResult(moved=[], skipped_legacy_absent=[], skipped_new_exists=[], backups=[])

    if is_migrated(new_state_file) and not force:
        result.already_migrated = True
        return result

    if not dry_run:
        HOST_STATE_DIR_for_home = new_state_file.parent
        HOST_STATE_DIR_for_home.mkdir(parents=True, exist_ok=True)

    for legacy, new in pairs:
        if not legacy.exists():
            result.skipped_legacy_absent.append(legacy)
            continue
        if new.exists() and not force:
            result.skipped_new_exists.append(new)
            continue
        if dry_run:
            result.moved.append((legacy, new))
            result.backups.append(legacy.with_suffix(legacy.suffix + ".migrated.bak"))
            continue
        new.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(legacy, new)
        backup = legacy.with_suffix(legacy.suffix + ".migrated.bak")
        legacy.rename(backup)
        result.moved.append((legacy, new))
        result.backups.append(backup)

    # Record the timestamp so subsequent runs short-circuit.
    if not dry_run:
        _stamp_migrated_at(new_state_file)

    return result


def auto_migrate_if_needed(home: Path | None = None) -> MigrationResult | None:
    """Called at daemon start. If migration hasn't run yet, do it; otherwise
    return ``None`` so the caller can stay quiet."""
    pairs = _migration_pairs(home)
    new_state_file = pairs[-1][1]
    if is_migrated(new_state_file):
        return None
    return migrate(dry_run=False, force=False, home=home)


# ---------- helpers ----------

def _stamp_migrated_at(state_file: Path) -> None:
    """Write/refresh the ``migrated_at`` field in monitor_state.json. Preserves
    any other fields a monitor may have written."""
    data: dict = {}
    if state_file.exists():
        try:
            loaded = json.loads(state_file.read_text())
            if isinstance(loaded, dict):
                data = loaded
        except Exception:
            data = {}
    data["migrated_at"] = time.time()
    write_json_atomic(state_file, data)


def render_summary(result: MigrationResult, *, dry_run: bool) -> str:
    """Human-readable summary of what migrate() did. Used by the CLI."""
    if result.already_migrated:
        return "Already migrated; nothing to do. Pass --force to re-run."
    prefix = "[dry-run] would move" if dry_run else "Moved"
    lines: list[str] = []
    if result.moved:
        lines.append(f"{prefix} {len(result.moved)} state file(s):")
        for legacy, new in result.moved:
            lines.append(f"  {legacy} -> {new}")
        if result.backups and not dry_run:
            lines.append("Legacy files renamed (recovery hatch):")
            for bak in result.backups:
                lines.append(f"  {bak}")
        elif result.backups and dry_run:
            lines.append("[dry-run] would rename legacy files:")
            for bak in result.backups:
                lines.append(f"  {bak}")
    if result.skipped_new_exists:
        lines.append(f"Skipped (target already exists, pass --force to overwrite): {len(result.skipped_new_exists)} file(s):")
        for p in result.skipped_new_exists:
            lines.append(f"  {p}")
    if result.skipped_legacy_absent and not result.moved:
        lines.append("No legacy state files found; nothing to migrate.")
    if not lines:
        lines.append("Nothing to do.")
    return "\n".join(lines)
