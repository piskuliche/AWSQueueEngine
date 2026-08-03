# Queue management helpers
import json
from .array_id import normalize_array_id
from .paths import QUEUE_FILE
from .queue_config import DEFAULT_QUEUE, host_is_eligible_for_item, normalize_queue_name
from .state_io import warn_unreadable, write_json_atomic
from .state_lock import state_lock

DEFAULT_PRIORITY = 0
DEFAULT_PREEMPT = False
DEFAULT_MPS = False
DEFAULT_RESUME_FIRST = False
LEGACY_PRIORITY_MAP = {
    "high": 100,
    "normal": 0,
}


def load_queue():
    if QUEUE_FILE.exists():
        try:
            data = json.loads(QUEUE_FILE.read_text())
            return data if isinstance(data, list) else []
        except Exception as exc:
            warn_unreadable(QUEUE_FILE, exc)
            return []
    return []


def save_queue(q):
    write_json_atomic(QUEUE_FILE, q)


def _normalize_priority(value):
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        priority_text = value.strip().lower()
        if priority_text in LEGACY_PRIORITY_MAP:
            return LEGACY_PRIORITY_MAP[priority_text]
        try:
            return int(priority_text)
        except ValueError:
            return DEFAULT_PRIORITY
    return DEFAULT_PRIORITY


def _normalize_hosts(hosts):
    if hosts is None:
        return None
    if isinstance(hosts, str):
        host_values = [hosts]
    elif isinstance(hosts, (list, tuple, set)):
        host_values = list(hosts)
    else:
        return None

    normalized_hosts = []
    seen = set()
    for host in host_values:
        if not isinstance(host, str):
            continue
        clean_host = host.strip()
        if not clean_host or clean_host in seen:
            continue
        normalized_hosts.append(clean_host)
        seen.add(clean_host)
    return normalized_hosts or None


def _normalize_preempt(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return DEFAULT_PREEMPT


def _normalize_mps(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return DEFAULT_MPS


def _normalize_payload_remote_path(value):
    if not isinstance(value, str):
        return None
    clean_value = value.strip()
    return clean_value or None


def _normalize_payload_s3_uri(value):
    if not isinstance(value, str):
        return None
    clean_value = value.strip()
    return clean_value or None


def _normalize_payload_size_bytes(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str):
        try:
            parsed = int(value.strip())
        except ValueError:
            return None
        return parsed if parsed >= 0 else None
    return None


def _normalize_resume_first(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return DEFAULT_RESUME_FIRST


def _normalize_resume_host(value):
    if not isinstance(value, str):
        return None
    clean_value = value.strip()
    return clean_value or None


def _normalize_job_id(value):
    if not isinstance(value, str):
        return None
    clean_value = value.strip()
    return clean_value or None


def normalize_job_item(item):
    if isinstance(item, str):
        return {
            "cmd": item,
            "payload": None,
            "priority": DEFAULT_PRIORITY,
            "queue": DEFAULT_QUEUE,
            "hosts": None,
            "preempt": DEFAULT_PREEMPT,
            "mps": DEFAULT_MPS,
            "payload_remote_path": None,
            "payload_s3_uri": None,
            "payload_size_bytes": None,
            "resume_first": DEFAULT_RESUME_FIRST,
            "resume_host": None,
            "job_id": None,
            "array_id": None,
        }

    if not isinstance(item, dict):
        return {
            "cmd": str(item),
            "payload": None,
            "priority": DEFAULT_PRIORITY,
            "queue": DEFAULT_QUEUE,
            "hosts": None,
            "preempt": DEFAULT_PREEMPT,
            "mps": DEFAULT_MPS,
            "payload_remote_path": None,
            "payload_s3_uri": None,
            "payload_size_bytes": None,
            "resume_first": DEFAULT_RESUME_FIRST,
            "resume_host": None,
            "job_id": None,
            "array_id": None,
        }

    submit_failures = item.get("submit_failures", 0)
    if not isinstance(submit_failures, int) or submit_failures < 0:
        submit_failures = 0
    return {
        "cmd": item.get("cmd"),
        "payload": item.get("payload"),
        "priority": _normalize_priority(item.get("priority", DEFAULT_PRIORITY)),
        "queue": normalize_queue_name(item.get("queue", DEFAULT_QUEUE)),
        "hosts": _normalize_hosts(item.get("hosts")),
        "preempt": _normalize_preempt(item.get("preempt", DEFAULT_PREEMPT)),
        "mps": _normalize_mps(item.get("mps", DEFAULT_MPS)),
        "payload_remote_path": _normalize_payload_remote_path(item.get("payload_remote_path")),
        "payload_s3_uri": _normalize_payload_s3_uri(item.get("payload_s3_uri")),
        "payload_size_bytes": _normalize_payload_size_bytes(item.get("payload_size_bytes")),
        "resume_first": _normalize_resume_first(item.get("resume_first", DEFAULT_RESUME_FIRST)),
        "resume_host": _normalize_resume_host(item.get("resume_host")),
        "job_id": _normalize_job_id(item.get("job_id")),
        "array_id": normalize_array_id(item.get("array_id")),
        "submit_failures": submit_failures,
    }


def build_resume_item(job_item, host, priority=None, mps=None):
    item = normalize_job_item(job_item)
    item["hosts"] = [host]
    item["queue"] = normalize_queue_name(item.get("queue", DEFAULT_QUEUE))
    item["resume_first"] = True
    item["resume_host"] = host
    item["submit_failures"] = 0
    if priority is not None:
        item["priority"] = _normalize_priority(priority)
    # mps=None preserves the job's existing setting; pass True/False to override.
    if mps is not None:
        item["mps"] = _normalize_mps(mps)
    return item


def enqueue_item(item):
    """Append one job to the back of the queue."""
    with state_lock():
        q = load_queue()
        q.append(normalize_job_item(item))
        save_queue(q)


def requeue_front(item):
    """Put a job back at the head of the queue — it should run next."""
    with state_lock():
        q = load_queue()
        q.insert(0, normalize_job_item(item))
        save_queue(q)


def requeue_back(item):
    """Put a job back at the tail of the queue — let everything else go first."""
    with state_lock():
        q = load_queue()
        q.append(normalize_job_item(item))
        save_queue(q)


def enqueue_items(items):
    """Append many items atomically, in one load-modify-save.

    A batch lands all-or-nothing and takes the lock once rather than once per
    job, so a 142-job submit cannot interleave with a dispatch or a qdel
    partway through.

    This originally existed as a mitigation: before the state lock, batching
    the writes was the only way to shrink (never close) the window in which the
    monitor could save a stale copy over a concurrent write. The lock closes
    that window properly, so what remains is the atomicity and the saved lock
    traffic.
    """
    normalized = [normalize_job_item(item) for item in items]
    if not normalized:
        return 0
    with state_lock():
        q = load_queue()
        q.extend(normalized)
        save_queue(q)
    return len(normalized)


# ---------- qdel selection ----------

class QueueSelectionError(Exception):
    """A qdel selector did not resolve to a unique set of queue entries.

    ``code`` is protocol-neutral on purpose: the RPC handler maps it onto an
    ``RpcError`` code while the host CLI just prints ``message``. ``tokens``
    carries the selectors that matched nothing, so callers can enrich the
    message with where those jobs actually went.
    """

    def __init__(self, code, message, tokens=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.tokens = list(tokens or [])


def _selector_kinds(job_ids, indices, queue, array_id=None):
    present = (
        ("job ids", job_ids),
        ("indices", indices),
        ("queue", queue),
        ("array", array_id),
    )
    return [name for name, value in present if value]


def _resolve_indices(q, indices):
    queue_size = len(q)
    unique_indices = sorted(set(indices))
    invalid = [i for i in unique_indices if i < 1 or i > queue_size]
    if invalid:
        raise QueueSelectionError(
            "conflict",
            f"invalid queue index(es): {', '.join(str(i) for i in invalid)}; queue size {queue_size}",
        )
    return [(idx - 1, str(idx)) for idx in unique_indices]


def _resolve_queue_name(q, queue):
    wanted = normalize_queue_name(queue)
    selection = [
        (pos, f"--queue {wanted}")
        for pos, raw_item in enumerate(q)
        if normalize_job_item(raw_item)["queue"] == wanted
    ]
    if not selection:
        raise QueueSelectionError("not_found", f"no queued jobs in queue {wanted!r}")
    return selection


def _resolve_job_ids(q, job_ids):
    """Match each token against job ids, exact first then unique prefix.

    Exact wins outright so a full job id is never reported as ambiguous even
    when it happens to prefix another one.
    """
    ids = [normalize_job_item(raw_item)["job_id"] for raw_item in q]
    folded = [(job_id or "").casefold() for job_id in ids]

    selection = {}
    missing = []
    for raw_token in job_ids:
        token = str(raw_token).strip()
        if not token:
            continue
        needle = token.casefold()
        matches = [pos for pos, value in enumerate(folded) if value and value == needle]
        if not matches:
            matches = [pos for pos, value in enumerate(folded) if value and value.startswith(needle)]
        if not matches:
            missing.append(token)
            continue
        if len(matches) > 1:
            candidates = ", ".join(ids[pos] for pos in matches)
            raise QueueSelectionError(
                "conflict",
                f"job id {token!r} matches {len(matches)} queued jobs: {candidates}",
            )
        selection.setdefault(matches[0], token)

    if missing:
        raise QueueSelectionError(
            "not_found",
            f"no queued job matching: {', '.join(missing)}",
            tokens=missing,
        )
    if not selection:
        raise QueueSelectionError("invalid_params", "provide at least one non-empty job id")
    return sorted(selection.items())


def _resolve_array_id(q, array_id):
    """Match every queued job carrying the batch tag `array_id`.

    Exact and case-insensitive, with **no prefix matching** — unlike
    :func:`_resolve_job_ids`, where a prefix is a convenience for typing one
    long id. Here a prefix would silently widen a destructive operation from
    one batch to every batch whose name starts the same way.
    """
    wanted = str(array_id).strip().casefold()
    selection = [
        (pos, f"--array {array_id}")
        for pos, raw_item in enumerate(q)
        if (normalize_job_item(raw_item)["array_id"] or "").casefold() == wanted
    ]
    if not selection:
        raise QueueSelectionError(
            "not_found",
            f"no queued jobs in array {str(array_id).strip()!r}",
        )
    return selection


def resolve_queue_selection(q, job_ids=None, indices=None, queue=None, array_id=None):
    """Resolve qdel selectors to ``[(position, token), ...]`` sorted by position.

    Exactly one selector kind may be supplied — mixing job ids with positions
    in a single command is the ambiguity this whole path exists to avoid.
    Resolution happens before anything is popped, so a bad selector leaves the
    queue untouched.
    """
    kinds = _selector_kinds(job_ids, indices, queue, array_id)
    if not kinds:
        raise QueueSelectionError(
            "invalid_params", "provide job id(s), indices, a queue name, or an array name"
        )
    if len(kinds) > 1:
        raise QueueSelectionError(
            "invalid_params",
            f"cannot combine {' and '.join(kinds)}; pick one selector",
        )
    if not q:
        raise QueueSelectionError("not_found", "queue is empty")

    if indices:
        return _resolve_indices(q, indices)
    if queue:
        return _resolve_queue_name(q, queue)
    if array_id:
        return _resolve_array_id(q, array_id)
    return _resolve_job_ids(q, job_ids)


def remove_queue_positions(q, selection):
    """Pop the selected positions from ``q``, persist, and report what went.

    Takes the ``[(position, token), ...]`` from :func:`resolve_queue_selection`
    and returns ``[(index_1based, item, token), ...]`` in ascending queue order.

    Operates on the list you hand it, so ``q`` must have been loaded under the
    state lock — see :func:`delete_queue_selection`, which is what callers
    should reach for.
    """
    removed = []
    for pos, token in sorted(selection, reverse=True):
        removed.append((pos + 1, normalize_job_item(q.pop(pos)), token))
    save_queue(q)
    return sorted(removed, key=lambda entry: entry[0])


def delete_queue_selection(job_ids=None, indices=None, queue=None, array_id=None):
    """Resolve a qdel selector and remove what it matches, atomically.

    The whole point is that resolution and removal see the *same* queue.
    Resolving against a copy loaded before the lock means a concurrent dispatch
    can shift every position between the two steps, and `qdel 3` then deletes
    somebody else's job. Raises :class:`QueueSelectionError` unchanged, having
    touched nothing.
    """
    with state_lock():
        q = load_queue()
        selection = resolve_queue_selection(
            q, job_ids=job_ids, indices=indices, queue=queue, array_id=array_id,
        )
        return remove_queue_positions(q, selection)


def claim_queued_job(job_id, fallback_item=None):
    """Remove one specific queued job, or return ``None`` if it is already gone.

    For a caller that picked a job from a queue snapshot and now wants to act on
    it — today only the monitor's preemption path. Between the snapshot and the
    claim the job may have been dispatched, qdel'd, or shifted position, so the
    entry is re-found by identity rather than by the index it used to sit at.

    ``fallback_item`` covers queue entries written before job ids existed: with
    no id to match on, the normalized item itself is the identity.
    """
    with state_lock():
        q = load_queue()
        position = _find_queued_position(q, job_id, fallback_item)
        if position is None:
            return None
        item = normalize_job_item(q.pop(position))
        save_queue(q)
        return item


def _find_queued_position(q, job_id, fallback_item=None):
    if job_id:
        for pos, raw_item in enumerate(q):
            if normalize_job_item(raw_item).get("job_id") == job_id:
                return pos
        return None
    if fallback_item is None:
        return None
    wanted = normalize_job_item(fallback_item)
    for pos, raw_item in enumerate(q):
        if normalize_job_item(raw_item) == wanted:
            return pos
    return None


def _is_host_eligible(item, host, queue_host_map=None):
    return host_is_eligible_for_item(item, host, queue_host_map)


def _select_best_index(q, host=None, queue_host_map=None):
    if host is not None:
        for idx, queued_item in enumerate(q):
            normalized_item = normalize_job_item(queued_item)
            if not _is_host_eligible(normalized_item, host, queue_host_map=queue_host_map):
                continue
            if normalized_item.get("resume_first") and normalized_item.get("resume_host") == host:
                return idx

    best_idx = None
    best_priority = None

    for idx, queued_item in enumerate(q):
        normalized_item = normalize_job_item(queued_item)
        if host is not None and not _is_host_eligible(normalized_item, host, queue_host_map=queue_host_map):
            continue

        priority = normalized_item["priority"]
        if best_idx is None or priority > best_priority:
            best_idx = idx
            best_priority = priority
    return best_idx


def _dequeue_index(q, idx):
    if idx is None:
        return None
    item = normalize_job_item(q.pop(idx))
    save_queue(q)
    return item


def dequeue():
    """Dequeue the highest-priority job (FIFO tie-breaker)."""
    with state_lock():
        q = load_queue()
        if not q:
            return None
        return _dequeue_index(q, _select_best_index(q))


def dequeue_for_host(host, queue_host_map=None):
    """Dequeue the highest-priority job eligible for the provided host.

    Load, select and pop all happen under one lock: selecting from a copy read
    beforehand would write back a queue that has forgotten anything enqueued in
    between.
    """
    with state_lock():
        q = load_queue()
        if not q:
            return None
        return _dequeue_index(q, _select_best_index(q, host=host, queue_host_map=queue_host_map))
