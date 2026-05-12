"""Central path resolution for state files.

In Phase 1, these are re-exports of the legacy paths under ``~/.aws_slurm_like_*``.
Phase 5 swaps them to ``~/.awsqe/host/*`` with one-shot migration; everything
that imports from here gets the new locations without per-call changes.
"""
from .config import (
    COMPLETED_FILE,
    DEFERRED_FILE,
    QUEUE_FILE,
    RUNNING_FILE,
)

__all__ = ["QUEUE_FILE", "RUNNING_FILE", "COMPLETED_FILE", "DEFERRED_FILE"]
