"""Authoritative paths for queue-host state, lock, and pidfile.

Phase 5 moves these from the legacy `~/.aws_slurm_like_*.json` set scattered
in `$HOME` to a single `~/.awsqe/host/` directory. One-shot migration of the
state files is handled by :mod:`awsqueueengine.host.migration`; legacy files
are preserved as `*.migrated.bak` on disk so an operator can roll back by
hand if something goes wrong.

The client-side root (``~/.awsqe/client/``) already exists for the config
file added in Phase 3; nothing client-side needs moving today.
"""
from pathlib import Path

# Roots.
AWSQE_ROOT = Path.home() / ".awsqe"
HOST_STATE_DIR = AWSQE_ROOT / "host"

# Persistent state files (one-shot migrated from ~/.aws_slurm_like_*.json).
QUEUE_FILE = HOST_STATE_DIR / "queue.json"
RUNNING_FILE = HOST_STATE_DIR / "running.json"
COMPLETED_FILE = HOST_STATE_DIR / "completed.json"
FAILED_FILE = HOST_STATE_DIR / "failed.json"
DEFERRED_FILE = HOST_STATE_DIR / "deferred.json"
MONITOR_STATE_FILE = HOST_STATE_DIR / "monitor_state.json"

# Ephemeral runtime files (not migrated; the new daemon writes new ones,
# and stale legacy files at ~/awsqueueengine.pid / ~/.aws_slurm_like.lock
# become orphans an operator can rm by hand).
LOCK_FILE = HOST_STATE_DIR / "lock"
PIDFILE = HOST_STATE_DIR / "pid"

__all__ = [
    "AWSQE_ROOT",
    "HOST_STATE_DIR",
    "QUEUE_FILE",
    "RUNNING_FILE",
    "COMPLETED_FILE",
    "FAILED_FILE",
    "DEFERRED_FILE",
    "MONITOR_STATE_FILE",
    "LOCK_FILE",
    "PIDFILE",
]
