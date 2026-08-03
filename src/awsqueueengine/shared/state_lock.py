"""The one lock that serializes read-modify-write on the queue host's state.

Atomic writes (:mod:`state_io`) guarantee a reader never sees a half-written
file. They do nothing about *lost updates*: two writers can both read, both
modify, and have the second replace win. On this host that is not theoretical —
every RPC runs in its own process (``ssh <host> awsqe-host rpc``), so `enqueue`,
`qdel` and `requeue_deferred` are genuinely concurrent with the monitor daemon.
The interleaving that motivated this file drops a submitted job outright:

===  ==========================================  ==========================
 #   monitor                                     RPC ``enqueue``
===  ==========================================  ==========================
 1   ``load_queue()`` → 10 jobs
 2                                               ``load_queue()`` → 10 jobs
 3                                               append, save → 11 on disk
 4   dispatch one, ``save_queue()`` → 9 on disk
===  ==========================================  ==========================

The submitter already saw ``Submitted <job_id>`` and has a ledger entry. The job
simply never runs, and later reports as ``missing``.

Every mutation therefore holds this lock **and re-reads inside it**. Taking the
lock around a stale in-memory copy fixes nothing: step 4 above still clobbers.

Design notes, since each is load-bearing:

- **One lock for every state file**, not one per file. The monitor mutates
  several per cycle; per-file locks buy no real concurrency here (critical
  sections are microseconds of local file I/O) and introduce lock-ordering
  bugs.
- **Distinct from** :data:`~awsqueueengine.shared.paths.LOCK_FILE`. That one is
  the daemon *singleton* lock, held for the monitor's entire process lifetime.
  Reusing it would mean every RPC blocked until the monitor exited.
- **Reentrant within a process.** ``flock`` is associated with the open file
  *description*, so a second ``open()`` + ``LOCK_EX`` in the same process
  self-deadlocks rather than nesting. Callers do nest — `requeue_deferred` pops
  from the deferred list and then enqueues — so the depth counter is required,
  not a nicety.
- **Blocking acquire, no timeout.** The kernel drops a ``flock`` when the
  holder dies, so a crashed process cannot wedge the host; the only way to
  block forever is a live holder that never lets go, which the invariant below
  rules out. A non-blocking attempt runs first so a slow holder gets logged.

**Invariant: nothing slow may run inside the lock.** No SSH, no email, no
``subprocess``, no network. The bug this fixes was as damaging as it was
precisely because the monitor carried its queue snapshot across an email send
and an SSH round trip — seconds of exposure per cycle. Critical sections here
are load, mutate in memory, save.
"""
from __future__ import annotations

import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

from .paths import HOST_STATE_DIR

try:  # pragma: no cover — exercised implicitly on POSIX
    import fcntl
except ImportError:  # pragma: no cover — Windows has no flock
    fcntl = None

#: Deliberately not ``paths.LOCK_FILE``; see the module docstring.
STATE_LOCK_FILE = HOST_STATE_DIR / "state.lock"

#: How long to wait for a contended lock before saying so on stderr. Every
#: critical section is local file I/O, so exceeding this means something is
#: wrong — a holder that violated the no-slow-work invariant, or a hung NFS
#: mount. We still wait; we just stop waiting silently.
SLOW_ACQUIRE_WARN_SECONDS = 5.0

# Serializes threads within this process and makes the depth counter safe to
# read. An RLock so a nested acquire on the same thread doesn't self-deadlock
# before the depth check even runs.
_local_lock = threading.RLock()
_depth = 0
_handle = None


@contextmanager
def state_lock(path=None):
    """Hold the state lock across a whole read-modify-write.

    Re-read the state you are about to modify *inside* this block. A copy
    loaded before the lock is stale by construction, and writing it back is the
    lost update this exists to prevent.

    Degrades to a plain mutex where ``fcntl`` is unavailable (Windows) or the
    lock file cannot be created: cross-process safety is gone, but refusing to
    touch the queue at all would be worse.
    """
    global _depth, _handle

    target = Path(path) if path is not None else STATE_LOCK_FILE
    with _local_lock:
        outermost = _depth == 0
        if outermost and fcntl is not None:
            _handle = _acquire_flock(target)
        _depth += 1
        try:
            yield
        finally:
            _depth -= 1
            if _depth == 0 and _handle is not None:
                _release_flock(_handle)
                _handle = None


def _acquire_flock(path):
    """Take the cross-process lock, or return ``None`` if we cannot."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = open(path, "w")
    except OSError as exc:
        print(
            f"[WARN] could not open state lock {path}: {exc}; "
            "proceeding without cross-process locking",
            file=sys.stderr,
            flush=True,
        )
        return None

    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return handle
    except BlockingIOError:
        pass
    except OSError as exc:
        handle.close()
        print(f"[WARN] could not lock {path}: {exc}", file=sys.stderr, flush=True)
        return None

    # Contended. Wait, but say so if the wait is long enough to mean a bug.
    waited_from = time.monotonic()
    try:
        fcntl.flock(handle, fcntl.LOCK_EX)
    except OSError as exc:
        handle.close()
        print(f"[WARN] could not lock {path}: {exc}", file=sys.stderr, flush=True)
        return None
    waited = time.monotonic() - waited_from
    if waited >= SLOW_ACQUIRE_WARN_SECONDS:
        print(
            f"[WARN] waited {waited:.1f}s for the state lock at {path}",
            file=sys.stderr,
            flush=True,
        )
    return handle


def _release_flock(handle):
    try:
        fcntl.flock(handle, fcntl.LOCK_UN)
    finally:
        handle.close()


__all__ = ["STATE_LOCK_FILE", "state_lock"]
