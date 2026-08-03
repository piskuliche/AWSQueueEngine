"""Local cache of worker job logs, for `awsqe-client jobs --fetch-logs`.

A job's stdout/stderr lands on the worker that ran it, at the deterministic
path ``{REMOTE_LOG_DIR}/{job_id}.log`` (see
:func:`awsqueueengine.shared.job_outcome.log_path_for_tag`). The tracked-job
ledger already records which worker that was, so the client has everything it
needs to go and get the file itself — no queue-host round trip involved.

Fetching is done with ``scp`` rather than through the JSON-over-SSH RPC on
purpose: a job log is unbounded (a multi-day run can produce gigabytes), and
the RPC buffers and JSON-escapes whole payloads in memory on both ends. That is
why the existing ``tail`` RPC clamps at 5000 lines. ``scp`` streams to disk.

Cache invalidation is the subtle part. The launch command redirects with ``>``,
so a requeued job *truncates* its log and reuses the same filename — and if the
retry landed on a different worker, the previous attempt's log is still sitting
on the old one. Caching on job_id alone would therefore serve a stale attempt
forever. The cache is keyed on ``host + finished_at`` (see :func:`cache_key`),
and only jobs in a terminal state are considered settled enough to cache at all.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from ..shared.config import SSH_TIMEOUT
from ..shared.job_outcome import log_path_for_tag
from ..shared.job_status import is_terminal
from .config import CONFIG_DIR

LOG_DIR = CONFIG_DIR / "logs"

#: Total bytes of cached logs to keep. Unlike the ledger, which is capped by
#: record count, this has to be capped by size — one pathological job can
#: outweigh thousands of ordinary ones.
MAX_LOG_CACHE_BYTES = 512 * 1024 * 1024

SCP_BIN = os.environ.get("AWSQUEUEENGINE_SCP_BIN", "scp")


def local_log_path(job_id, root=None):
    """Where this job's log is cached locally."""
    return (root or LOG_DIR) / f"{job_id}.log"


def cache_key(record):
    """Identity of the *attempt* whose log we would be caching.

    A requeue reuses the job id and truncates the remote log, so the id alone
    is not enough to tell one attempt's output from another's.
    """
    return f"{record.get('host') or '?'}@{record.get('finished_at') or record.get('started_at') or ''}"


def is_cached(record, root=None):
    """True when we already hold *this attempt's* log on disk."""
    if record.get("log_fetched_for") != cache_key(record):
        return False
    path = record.get("log_path")
    return bool(path) and Path(path).exists()


def should_fetch(record, root=None):
    """Whether `record` is worth fetching a log for right now.

    Skips jobs with no worker recorded (never dispatched), jobs whose log we
    already hold for this attempt, and jobs we have already established have no
    log left on the worker. Non-terminal jobs are always re-fetched: their log
    is still being written to, so "already downloaded" would be a lie.
    """
    if not record.get("host"):
        return False
    if record.get("log_missing_for") == cache_key(record):
        return False
    if not is_terminal(record.get("status")):
        return True
    return not is_cached(record, root=root)


def fetch_log(job_id, host, *, dest=None, root=None, timeout=SSH_TIMEOUT, runner=None):
    """Copy one job's log off its worker. Returns ``{ok, path, reason, detail}``.

    ``reason`` is ``"missing"`` when the worker no longer has the file (hosts
    get recycled and ``manager_jobs`` gets cleaned), versus ``"error"`` for a
    genuine transfer failure — the caller records those differently, so a
    vanished log isn't retried on every run.
    """
    target = Path(dest) if dest else local_log_path(job_id, root=root)
    remote = f"{host}:{log_path_for_tag(job_id)}"
    argv = [SCP_BIN, "-q", "-o", "BatchMode=yes", remote, str(target)]
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        result = (runner or _run)(argv, timeout)
    except OSError as exc:
        return {"ok": False, "path": None, "reason": "error", "detail": str(exc)}
    if result["returncode"] == 0:
        return {"ok": True, "path": str(target), "reason": "", "detail": ""}
    detail = (result.get("stderr") or "").strip()
    # scp exits 1 for both "no such file" and "host unreachable"; only the
    # message distinguishes them, and only the first is worth remembering.
    lowered = detail.lower()
    missing = "no such file" in lowered or "not a regular file" in lowered
    return {
        "ok": False,
        "path": None,
        "reason": "missing" if missing else "error",
        "detail": detail or f"scp exited {result['returncode']}",
    }


def _run(argv, timeout):
    try:
        completed = subprocess.run(argv, capture_output=True, text=True,
                                   timeout=timeout, check=False)
        return {"returncode": completed.returncode, "stderr": completed.stderr}
    except subprocess.TimeoutExpired:
        return {"returncode": 124, "stderr": "scp timeout"}


def prune_log_cache(max_bytes=None, root=None):
    """Drop the oldest cached logs once the cache exceeds its size budget.

    Returns the number of files removed. Oldest-first by mtime, which for a
    log cache approximates least-recently-relevant.
    """
    directory = root or LOG_DIR
    budget = MAX_LOG_CACHE_BYTES if max_bytes is None else max_bytes
    if not directory.exists():
        return 0
    entries = []
    total = 0
    for path in directory.glob("*.log"):
        try:
            stat = path.stat()
        except OSError:
            continue
        entries.append((stat.st_mtime, stat.st_size, path))
        total += stat.st_size
    if total <= budget:
        return 0
    removed = 0
    for _, size, path in sorted(entries):
        if total <= budget:
            break
        try:
            path.unlink()
        except OSError:
            continue
        total -= size
        removed += 1
    return removed


def forget_logs(job_ids, root=None):
    """Delete cached logs for jobs being dropped from the ledger."""
    removed = 0
    for job_id in job_ids or []:
        path = local_log_path(job_id, root=root)
        try:
            path.unlink()
            removed += 1
        except OSError:
            continue
    return removed


def scp_available():
    """False when there is no scp on PATH, so the caller can say so once."""
    return shutil.which(SCP_BIN) is not None


def warn_no_scp():
    print(
        f"[WARN] {SCP_BIN} not found on PATH; cannot fetch job logs. "
        "Set AWSQUEUEENGINE_SCP_BIN if it lives elsewhere.",
        file=sys.stderr,
        flush=True,
    )
