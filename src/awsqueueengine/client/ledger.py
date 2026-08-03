"""The client's record of jobs *this machine* submitted.

The queue host keeps no notion of who submitted what — `list`, `qstat` and
`failed` are all global views — so a submitter that wants to follow its own
jobs has to remember them itself. That is this file: a ledger at
``~/.awsqe/client/jobs.json``, appended on every `awsqe-client submit` and
refreshed from the queue host by `awsqe-client jobs`.

Relationship to ``run.info``: a payload directory's ``run.info`` is that
*payload's* record and is written only when a job has a payload directory; the
ledger is the *client's* record and covers every submit, including payload-less
ones. Both apply the same merge rules, via :func:`merge_state` here, so the two
views can't disagree about a job they both know.

Timestamps: ``submitted_at`` is an epoch float, because the date filters need a
total order. Everything the host reports (``started_at``, ``finished_at``) stays
as the host's own preformatted local-time string — those are display-only and
must never be compared against the client's clock.
"""
from __future__ import annotations

import json
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

from ..shared.job_status import (
    ALL_STATUSES,
    FAILED,
    MISSING,
    SUBMITTED,
    is_terminal,
)
from .config import CONFIG_DIR

try:  # pragma: no cover — exercised implicitly on POSIX
    import fcntl
except ImportError:  # pragma: no cover — Windows has no flock
    fcntl = None


LEDGER_PATH = CONFIG_DIR / "jobs.json"
LEDGER_VERSION = 1

#: Ceiling on tracked jobs, mirroring ``failure_state.MAX_FAILED_RECORDS``.
#: Only *terminal* records are ever evicted — see :func:`prune_records`.
MAX_TRACKED_RECORDS = 2000

#: Fields the client owns. A refresh from the queue host never overwrites these
#: (the host has no opinion about them, and for `queue_host` it can't).
#:
#: `array_id` is here even though the queue host learns it too: the submitter
#: chose the tag, so the host's copy is derivative. Keeping it client-owned also
#: sidesteps the one asymmetry in `merge_host_fields` — empty values never
#: overwrite, so a host-owned tag could never *clear* a stale one, only replace
#: it with a different one, silently.
_CLIENT_OWNED = ("job_id", "submitted_at", "queue_host", "payload", "payload_s3_uri", "array_id")

#: Written by the host only while a job is failed; a retry that succeeds must
#: not leave the previous attempt's fields behind.
_FAILURE_FIELDS = ("failure_reason", "failure_detail", "exit_code")


class LedgerSelectionError(Exception):
    """A job-id token matched more than one tracked job."""

    def __init__(self, message, candidates=None):
        super().__init__(message)
        self.message = message
        self.candidates = list(candidates or [])


# ---------- load / save ----------

def _read_raw(path):
    """Return ``(records, ok)``. ``ok`` is False when the file exists but is unreadable."""
    if not path.exists():
        return [], True
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        print(f"[WARN] failed to parse {path}: {exc}", file=sys.stderr, flush=True)
        return [], False
    # Accept a bare list as well as the versioned envelope, so a hand-edited or
    # older-format file still loads.
    if isinstance(data, dict):
        raw_records = data.get("jobs")
    else:
        raw_records = data
    if not isinstance(raw_records, list):
        print(f"[WARN] {path} has no job list; ignoring it", file=sys.stderr, flush=True)
        return [], False
    return [_normalize_record(item) for item in raw_records if isinstance(item, dict)], True


def _normalize_record(item):
    """Coerce the fields we depend on, preserving everything else.

    Unknown keys survive untouched so an older client can't silently strip a
    field a newer one added.
    """
    record = dict(item)
    record["job_id"] = str(record.get("job_id") or "")
    record["status"] = str(record.get("status") or SUBMITTED)
    submitted_at = record.get("submitted_at")
    if isinstance(submitted_at, bool) or not isinstance(submitted_at, (int, float)):
        record["submitted_at"] = 0.0
    else:
        record["submitted_at"] = float(submitted_at)
    return record


def load_ledger(path=None):
    """All tracked jobs, oldest first. A missing or unreadable file yields ``[]``."""
    records, _ = _read_raw(path or LEDGER_PATH)
    return [r for r in records if r["job_id"]]


def prune_records(records):
    """Trim to :data:`MAX_TRACKED_RECORDS`, evicting only *terminal* jobs.

    A plain insertion-order cap would drop the oldest record regardless of
    state, which during a large burst could evict a job that is still running —
    exactly the record you most want kept. Non-terminal records are therefore
    always retained, even if that leaves the ledger above the cap.
    """
    if len(records) <= MAX_TRACKED_RECORDS:
        return list(records)
    keep_flags = [not is_terminal(r.get("status")) for r in records]
    droppable = len(records) - MAX_TRACKED_RECORDS
    pruned = []
    for record, keep in zip(records, keep_flags):
        if keep or droppable <= 0:
            pruned.append(record)
        else:
            droppable -= 1
    return pruned


def save_ledger(records, path=None):
    """Atomically write the ledger, pruning first. Returns the path written."""
    target = path or LEDGER_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {"version": LEDGER_VERSION, "jobs": prune_records(records)}
    # `.tmp.<pid>` rather than a shared `.tmp`, so a stale temp file left by a
    # killed process can never become the source of somebody else's replace.
    tmp = target.with_suffix(target.suffix + f".tmp.{os.getpid()}")
    tmp.write_text(json.dumps(payload, indent=2) + "\n")
    os.replace(tmp, target)
    return target


# ---------- locking ----------

@contextmanager
def _locked(path):
    """Hold an advisory lock across a whole read-modify-write.

    Atomic replace alone does not make concurrent submits safe: two of them can
    both read, both append, and the second write wins, silently losing a job.
    Degrades to a no-op where ``fcntl`` is unavailable or the lock file can't be
    created — a lost append beats refusing to record the job at all.
    """
    if fcntl is None:
        yield
        return
    lock_path = path.with_suffix(path.suffix + ".lock")
    handle = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = open(lock_path, "w")
        fcntl.flock(handle, fcntl.LOCK_EX)
    except OSError:
        if handle is not None:
            handle.close()
        yield
        return
    try:
        yield
    finally:
        try:
            fcntl.flock(handle, fcntl.LOCK_UN)
        finally:
            handle.close()


@contextmanager
def _mutating(path):
    """Lock, hand the caller the current records, and write back what it returns.

    The re-read happens *inside* the lock, so an update computed before a
    concurrent submit still can't clobber the newly appended record.

    Writes only when something actually changed, so `awsqe-client jobs` against
    a settled ledger stays a pure read.
    """
    target = path or LEDGER_PATH
    with _locked(target):
        records, ok = _read_raw(target)
        if not ok:
            _quarantine(target)
        box = [r for r in records if r["job_id"]]
        before = [dict(r) for r in box]
        yield box
        if box != before or not ok:
            save_ledger(box, target)


def _quarantine(path):
    """Move an unreadable ledger aside once, so the next write doesn't erase it."""
    backup = path.with_suffix(path.suffix + ".corrupt")
    try:
        os.replace(path, backup)
        print(f"[WARN] moved unreadable ledger to {backup}", file=sys.stderr, flush=True)
    except OSError:
        pass


# ---------- mutations ----------

def build_submission_record(*, job_id, queue_host, queue=None, cmd="", payload="",
                            payload_s3_uri="", array_id="", array_size=None,
                            submitted_at=None):
    """The record shape for a freshly enqueued job, or ``None`` without an id.

    Split out from :func:`record_submission` so a batch submit can build many
    records and write them in one locked cycle without the shape being defined
    twice.

    `array_size` is how many jobs the batch was submitted with, recorded only
    when the submitter knows — a one-invocation batch submit does, a shell loop
    calling submit N times does not. It exists so the grouped view can say
    ``130/142`` when the 2000-record cap has evicted part of a finished batch,
    rather than quietly reporting a smaller batch than was actually run.
    """
    if not job_id:
        return None
    record = {
        "job_id": job_id,
        "submitted_at": float(submitted_at if submitted_at is not None else time.time()),
        "queue_host": queue_host or "",
        "queue": queue or "",
        "cmd": cmd or "",
        "payload": str(payload or ""),
        "payload_s3_uri": payload_s3_uri or "",
        "array_id": array_id or "",
        "status": SUBMITTED,
        "updated_at": 0.0,
    }
    if array_size:
        record["array_size"] = int(array_size)
    return record


def record_submission(*, path=None, **fields):
    """Start tracking a freshly enqueued job. Returns the record, or ``None``.

    A failure here must never fail the submit: by the time this runs the job is
    already on the queue host, so we warn and carry on rather than raise.
    """
    record = build_submission_record(**fields)
    if record is None:
        return None
    target = path or LEDGER_PATH
    try:
        with _mutating(target) as records:
            records.append(record)
    except OSError as exc:
        print(
            f"[WARN] could not record {record['job_id']} in {target}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return None
    return record


def apply_states(states, path=None):
    """Merge queue-host state into existing records. Returns how many changed.

    Updates only — a job id the ledger doesn't already track is ignored, so
    inspecting somebody else's job never starts tracking it here.
    """
    if not states:
        return 0
    changed = 0
    try:
        with _mutating(path) as records:
            for index, record in enumerate(records):
                if record["job_id"] not in states:
                    continue
                merged = merge_state(record, states[record["job_id"]])
                if merged != record:
                    records[index] = merged
                    changed += 1
    except OSError as exc:
        print(f"[WARN] could not update {path or LEDGER_PATH}: {exc}", file=sys.stderr, flush=True)
        return 0
    return changed


def apply_state(job_id, state, path=None):
    """Merge one job's state. True when the record existed and changed."""
    return bool(job_id) and apply_states({job_id: state}, path=path) > 0


def record_log_result(job_id, *, log_path=None, fetched_for=None, missing_for=None, path=None):
    """Remember the outcome of a log fetch against a tracked job.

    `fetched_for` / `missing_for` carry the attempt identity (see
    ``logs.cache_key``) so a requeue invalidates both the cached copy and the
    "there is no log" answer, rather than either sticking forever.
    """
    if not job_id:
        return False
    changed = False
    try:
        with _mutating(path) as records:
            for record in records:
                if record["job_id"] != job_id:
                    continue
                if log_path:
                    record["log_path"] = str(log_path)
                    record["log_fetched_for"] = fetched_for
                    record.pop("log_missing_for", None)
                elif missing_for:
                    record["log_missing_for"] = missing_for
                changed = True
    except OSError as exc:
        print(f"[WARN] could not update {path or LEDGER_PATH}: {exc}", file=sys.stderr, flush=True)
        return False
    return changed


def mark_status(job_ids, status, path=None):
    """Force a status onto tracked records. Returns how many changed."""
    wanted = {job_id for job_id in (job_ids or []) if job_id}
    if not wanted:
        return 0
    changed = 0
    try:
        with _mutating(path) as records:
            for record in records:
                if record["job_id"] in wanted and record.get("status") != status:
                    record["status"] = status
                    record["updated_at"] = time.time()
                    record.pop("queue_position", None)
                    changed += 1
    except OSError as exc:
        print(f"[WARN] could not update {path or LEDGER_PATH}: {exc}", file=sys.stderr, flush=True)
        return 0
    return changed


def forget(job_ids=None, before=None, path=None):
    """Stop tracking jobs. Returns how many records were removed.

    This only forgets — it never touches the job on the queue host.
    """
    drop = {job_id for job_id in (job_ids or []) if job_id}
    if not drop and before is None:
        return 0
    removed = 0
    with _mutating(path) as records:
        keep = []
        for record in records:
            if record["job_id"] in drop:
                removed += 1
            elif before is not None and record["submitted_at"] < before:
                removed += 1
            else:
                keep.append(record)
        records[:] = keep
    return removed


# ---------- queries ----------

def find_record(job_id, records=None, path=None):
    """Resolve a job id or unique prefix to a record, or ``None`` if unknown.

    Exact matches win outright, so a full job id is never reported ambiguous
    just because it happens to prefix another. Mirrors how `qdel` resolves ids
    against the live queue (`shared.queue._resolve_job_ids`); the two resolve
    against different sets, so a prefix unique to one may be ambiguous in the
    other.
    """
    token = str(job_id or "").strip()
    if not token:
        return None
    pool = load_ledger(path) if records is None else records
    needle = token.casefold()
    exact = [r for r in pool if r["job_id"].casefold() == needle]
    if exact:
        return exact[-1]
    matches = [r for r in pool if r["job_id"].casefold().startswith(needle)]
    if not matches:
        return None
    if len(matches) > 1:
        candidates = [r["job_id"] for r in matches]
        raise LedgerSelectionError(
            f"job id {token!r} matches {len(matches)} tracked jobs: {', '.join(candidates)}",
            candidates=candidates,
        )
    return matches[0]


def merge_host_fields(base, state):
    """Overlay a queue-host `state` dict onto `base`, without ledger bookkeeping.

    The one definition of "how host state updates a local record", shared by
    the ledger and by ``cmd_info``'s ``run.info`` rewrite so the two views can't
    drift apart:

    - empty or ``None`` values never overwrite what we already have;
    - client-owned fields are never touched;
    - a status other than ``failed`` clears the failure fields, so a successful
      retry doesn't keep reporting the previous attempt's error.
    """
    merged = dict(base)
    for key, value in state.items():
        if value is None or value == "" or key in _CLIENT_OWNED:
            continue
        merged[key] = value
    if merged.get("status") != FAILED:
        for key in _FAILURE_FIELDS:
            merged.pop(key, None)
    return merged


def merge_state(record, state, *, now=None):
    """Return `record` updated with a queue-host `state` dict.

    Adds the ledger's own bookkeeping on top of :func:`merge_host_fields`:
    an ``updated_at`` stamp when something actually changed, and the `missing`
    handling for ``state is None`` (the host has no record of the job),
    timestamped so a caller can say how long it has been unresolvable.
    """
    stamp = now if now is not None else time.time()

    if state is None:
        merged = dict(record)
        if merged.get("status") != MISSING:
            merged["status"] = MISSING
            merged["updated_at"] = stamp
            merged.setdefault("missing_since", stamp)
        return merged

    merged = merge_host_fields(record, state)
    merged.pop("missing_since", None)
    if merged != record:
        merged["updated_at"] = stamp
    return merged


def filter_records(records, *, statuses=None, queues=None, arrays=None,
                   since=None, until=None, limit=None):
    """Filter and sort tracked jobs for display: newest submission first.

    `since` is inclusive and `until` exclusive, both against ``submitted_at``
    (the *client's* clock — the host's timestamps are strings in the host's
    zone and are never compared here). `queues` and `arrays` are matched
    case-insensitively; callers should normalize the names first. `limit` is
    applied last; ``0``, ``None`` or a negative value means no limit.
    """
    selected = list(records)
    if statuses:
        selected = [r for r in selected if r.get("status") in statuses]
    if queues:
        wanted = {str(q).casefold() for q in queues}
        selected = [r for r in selected if str(r.get("queue") or "").casefold() in wanted]
    if arrays:
        wanted = {str(a).casefold() for a in arrays}
        selected = [r for r in selected if str(r.get("array_id") or "").casefold() in wanted]
    if since is not None:
        selected = [r for r in selected if r["submitted_at"] >= since]
    if until is not None:
        selected = [r for r in selected if r["submitted_at"] < until]
    selected.sort(key=lambda r: r["submitted_at"], reverse=True)
    # `limit > 0`, not just truthy: a negative would silently slice off the tail.
    if limit and limit > 0:
        selected = selected[:limit]
    return selected


# ---------- batch grouping ----------

def summarize_statuses(records):
    """``[(status, count), ...]`` for a batch, in a fixed order.

    Ordered by :data:`~awsqueueengine.shared.job_status.ALL_STATUSES` rather
    than by count, so the summary cell for a batch doesn't reshuffle itself
    between two runs a second apart. A status this client doesn't know (a newer
    queue host invented it) sorts last rather than being dropped.
    """
    counts = {}
    for record in records:
        status = str(record.get("status") or "")
        counts[status] = counts.get(status, 0) + 1
    order = {name: index for index, name in enumerate(ALL_STATUSES)}
    return sorted(counts.items(), key=lambda kv: (order.get(kv[0], len(order)), kv[0]))


def _distinct(records, key):
    """Distinct non-empty values of `key`, in first-seen order."""
    seen = []
    for record in records:
        value = str(record.get(key) or "")
        if value and value not in seen:
            seen.append(value)
    return seen


def group_by_array(records):
    """Split `records` into ``(groups, ungrouped)`` for the batch view.

    A batch is grouped on ``array_id`` alone, never on ``(array_id, queue)``:
    splitting one batch across rows because two of its jobs went to different
    queues would recreate exactly the noise the grouped view exists to remove.
    Heterogeneity is reported inside the row instead — see ``queues`` and
    ``queue_hosts``, which callers render as the single shared name or a marker.

    Groups are ordered by their *most recent* submission so a batch still being
    submitted stays at the top, matching :func:`filter_records`' newest-first
    contract; ``submitted_at`` on the group is the *earliest*, which is when the
    user fired the batch off. Ungrouped records keep the order they arrived in.
    """
    order = []
    by_array = {}
    ungrouped = []
    for record in records:
        array_id = str(record.get("array_id") or "")
        if not array_id:
            ungrouped.append(record)
            continue
        if array_id not in by_array:
            by_array[array_id] = []
            order.append(array_id)
        by_array[array_id].append(record)

    groups = []
    for array_id in order:
        members = by_array[array_id]
        stamps = [r["submitted_at"] for r in members]
        # The largest recorded size wins: every member of a batch submitted in
        # one invocation carries the same value, but a record whose batch was
        # partly evicted is still worth believing over a missing value.
        sizes = [r.get("array_size") for r in members if isinstance(r.get("array_size"), int)]
        groups.append({
            "array_id": array_id,
            "count": len(members),
            "array_size": max(sizes) if sizes else None,
            "submitted_at": min(stamps),
            "last_submitted_at": max(stamps),
            "status_counts": summarize_statuses(members),
            "queues": _distinct(members, "queue"),
            "queue_hosts": _distinct(members, "queue_host"),
            "records": members,
        })
    groups.sort(key=lambda g: g["last_submitted_at"], reverse=True)
    return groups, ungrouped
