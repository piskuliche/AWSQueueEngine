"""RPC handlers for ``awsqe-host rpc``.

The handlers operate on the queue host's local state files. They return
plain dicts and raise :class:`RpcError` on application-level failures —
no printing, no ``sys.exit``. The ``dispatch`` function below is the
single entry point used by both the CLI (stdin → stdout) and tests.
"""
from __future__ import annotations

import json
import sys
from typing import Any, Callable

from ..shared.deferred_state import (
    load_deferred_jobs,
    pop_all_deferred,
    pop_deferred_by_indices,
)
from ..shared.failure_state import load_failed_jobs
from ..shared.job_lookup import (
    enrich_selection_message,
    lookup_job_state,
    lookup_job_states,
    running_members_of_array,
)
from ..shared.protocol import (
    PROTOCOL_VERSION,
    RpcError,
    make_error,
    make_ok,
)
from ..shared.queue import (
    QueueSelectionError,
    enqueue_item,
    load_queue,
    normalize_job_item,
    remove_queue_positions,
    resolve_queue_selection,
    save_queue,
)
from ..shared.queue_config import (
    DEFAULT_QUEUE,
    QueueConfigSource,
    normalize_queue_name,
)
from ..shared.running_state import load_running_jobs
from ..shared.worker_actions import new_job_tag, tail_remote_log
from .monitor import clear_host_cooldowns, get_host_cooldowns


#: Ceiling on how many job ids one `job_info_batch` call will resolve. The
#: request arrives on stdin rather than argv, so ARG_MAX doesn't apply; this is
#: about bounding the response, which runs ~300 bytes per resolved job.
MAX_JOB_INFO_BATCH = 500


# ---------- helpers ----------

def _require_dict(params: Any) -> dict:
    if not isinstance(params, dict):
        raise RpcError("invalid_params", "params must be an object")
    return params


def _require_str(params: dict, name: str) -> str:
    value = params.get(name)
    if not isinstance(value, str) or not value.strip():
        raise RpcError("invalid_params", f"missing or empty string param: {name}")
    return value.strip()


def _optional_str(params: dict, name: str) -> str | None:
    value = params.get(name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RpcError("invalid_params", f"param {name} must be a string")
    clean = value.strip()
    return clean or None


def _optional_int(params: dict, name: str) -> int | None:
    value = params.get(name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise RpcError("invalid_params", f"param {name} must be an integer")
    return value


def _optional_bool(params: dict, name: str) -> bool:
    value = params.get(name)
    if value is None:
        return False
    if not isinstance(value, bool):
        raise RpcError("invalid_params", f"param {name} must be a boolean")
    return value


def _optional_string_list(params: dict, name: str) -> list[str] | None:
    value = params.get(name)
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise RpcError("invalid_params", f"param {name} must be a list of strings")
    return [v for v in value if v.strip()]


def _optional_int_list(params: dict, name: str) -> list[int]:
    value = params.get(name) or []
    if not isinstance(value, list) or not all(isinstance(v, int) and not isinstance(v, bool) for v in value):
        raise RpcError("invalid_params", f"param {name} must be a list of integers")
    return list(value)


def _load_queue_host_map() -> dict[str, list[str]]:
    try:
        source = QueueConfigSource()
        return source.refresh()
    except ValueError as exc:
        raise RpcError("internal", f"invalid queue host configuration: {exc}") from exc


# ---------- handlers ----------

def handle_enqueue(params: dict) -> dict:
    params = _require_dict(params)
    command = _require_str(params, "cmd")
    queue_name = normalize_queue_name(_optional_str(params, "queue") or DEFAULT_QUEUE)
    hosts = _optional_string_list(params, "hosts")
    priority = _optional_int(params, "priority")
    high_priority = _optional_bool(params, "high_priority")
    preempt = _optional_bool(params, "preempt")
    mps = _optional_bool(params, "mps")
    payload = _optional_str(params, "payload")
    payload_s3_uri = _optional_str(params, "payload_s3_uri")
    payload_size_bytes = _optional_int(params, "payload_size_bytes")
    job_id = _optional_str(params, "job_id") or new_job_tag()
    array_id = _optional_str(params, "array_id")

    queue_host_map = _load_queue_host_map()
    if queue_name not in queue_host_map:
        valid = ", ".join(sorted(queue_host_map)) if queue_host_map else "(none)"
        raise RpcError("invalid_params", f"unknown queue {queue_name!r}. Valid queues: {valid}")

    if hosts:
        valid_hosts = set(queue_host_map.get(queue_name, []))
        invalid = sorted({h for h in hosts if h not in valid_hosts})
        if invalid:
            raise RpcError("invalid_params", f"invalid host(s): {', '.join(invalid)}")

    if priority is None:
        priority = 100 if high_priority else 0

    local_payload_path = None if payload_s3_uri else payload
    item = {
        "cmd": command,
        "payload": local_payload_path,
        "priority": priority,
        "queue": queue_name,
        "hosts": hosts,
        "preempt": preempt,
        "mps": mps,
        "payload_s3_uri": payload_s3_uri,
        "payload_size_bytes": payload_size_bytes,
        "job_id": job_id,
        "array_id": array_id,
    }
    enqueue_item(item)
    return {"job_id": job_id, "queue": queue_name, "hosts": hosts, "array_id": array_id}


def handle_list(params: dict) -> dict:
    _require_dict(params)
    jobs = [normalize_job_item(item) for item in load_queue()]
    return {"jobs": jobs}


def handle_qstat(params: dict) -> dict:
    _require_dict(params)
    running = {}
    for host, raw_item in load_running_jobs().items():
        item = normalize_job_item(raw_item)
        started_at = raw_item.get("started_at") if isinstance(raw_item, dict) else None
        item["started_at"] = started_at
        running[host] = item
    return {"running": running}


def handle_qdel(params: dict) -> dict:
    """Delete queued jobs selected by job id, queue name, array name, or position.

    ``indices`` keeps its original meaning so an older client still works
    against this handler unchanged.
    """
    params = _require_dict(params)
    job_ids = _optional_string_list(params, "job_ids") or []
    indices = _optional_int_list(params, "indices")
    queue = _optional_str(params, "queue")
    array_id = _optional_str(params, "array_id")

    q = load_queue()
    try:
        selection = resolve_queue_selection(
            q, job_ids=job_ids, indices=indices, queue=queue, array_id=array_id,
        )
    except QueueSelectionError as exc:
        raise RpcError(exc.code, enrich_selection_message(exc.message, exc.tokens)) from exc

    removed = remove_queue_positions(q, selection)
    result = {
        "removed": [
            {"index": idx, "item": item, "selector": token} for idx, item, token in removed
        ]
    }
    if array_id:
        result["running"] = running_members_of_array(array_id)
    return result


def handle_deferred_list(params: dict) -> dict:
    _require_dict(params)
    jobs = []
    for raw in load_deferred_jobs():
        item = normalize_job_item(raw)
        if isinstance(raw, dict):
            item["deferred_at"] = raw.get("deferred_at")
            item["last_error"] = raw.get("last_error") or ""
            item["last_host"] = raw.get("last_host") or ""
        jobs.append(item)
    return {"jobs": jobs}


def handle_requeue_deferred(params: dict) -> dict:
    params = _require_dict(params)
    indices = _optional_int_list(params, "indices")
    all_flag = _optional_bool(params, "all")
    drop = _optional_bool(params, "drop")

    if all_flag and indices:
        raise RpcError("invalid_params", "cannot combine all=true with explicit indices")
    if not all_flag and not indices:
        raise RpcError("invalid_params", "provide indices or set all=true")

    jobs = load_deferred_jobs()
    if not jobs:
        return {"moved": [], "action": "dropped" if drop else "requeued"}

    if all_flag:
        popped = pop_all_deferred()
    else:
        size = len(jobs)
        invalid = [i for i in indices if i < 1 or i > size]
        if invalid:
            raise RpcError(
                "conflict",
                f"invalid deferred index(es): {', '.join(str(i) for i in sorted(set(invalid)))}; size {size}",
            )
        popped = pop_deferred_by_indices(indices)

    moved = []
    for idx, raw_item in popped:
        item = normalize_job_item(raw_item)
        item["submit_failures"] = 0
        if not drop:
            enqueue_item(item)
        moved.append({"index": idx, "item": item})
    return {"moved": moved, "action": "dropped" if drop else "requeued"}


def handle_failed_list(params: dict) -> dict:
    """Most recent failed jobs, newest first.

    ``limit`` caps how many come back (default 50) and ``log`` opts into the
    captured log tail, which is far too big to ship for every record.
    """
    params = _require_dict(params)
    limit = _optional_int(params, "limit")
    if limit is None:
        limit = 50
    limit = max(1, min(limit, 1000))
    include_log = _optional_bool(params, "log")
    job_id = _optional_str(params, "job_id")

    jobs = []
    for raw in reversed(load_failed_jobs()):
        if not isinstance(raw, dict):
            continue
        if job_id and raw.get("job_id") != job_id:
            continue
        record = dict(raw)
        if not include_log:
            record.pop("log_tail", None)
        jobs.append(record)
        if len(jobs) >= limit:
            break
    return {"jobs": jobs}


def handle_list_cooldowns(params: dict) -> dict:
    _require_dict(params)
    cooldowns = get_host_cooldowns()
    return {"cooldowns": {host: float(ts) for host, ts in cooldowns.items()}}


def handle_enable_host(params: dict) -> dict:
    params = _require_dict(params)
    hosts = _optional_string_list(params, "hosts")
    all_flag = _optional_bool(params, "all")
    if all_flag and hosts:
        raise RpcError("invalid_params", "cannot combine all=true with explicit hosts")
    if not all_flag and not hosts:
        raise RpcError("invalid_params", "provide hosts or set all=true")
    cleared = clear_host_cooldowns(hosts=hosts or None, all_hosts=all_flag)
    return {"cleared": cleared}


def handle_job_info(params: dict) -> dict:
    params = _require_dict(params)
    job_id = _require_str(params, "job_id")
    state = lookup_job_state(job_id)
    return {"state": state}


def handle_job_info_batch(params: dict) -> dict:
    """Resolve many job ids in one round trip, for the client's tracked-job list.

    ``states`` holds an entry for every id we processed, with an explicit
    ``null`` for ids the host has no record of — so the client can tell "not
    found" from "the host didn't answer for this one".
    """
    params = _require_dict(params)
    job_ids = _optional_string_list(params, "job_ids") or []
    # _optional_string_list drops blanks but doesn't strip, and _require_str's
    # convention is that a param is its stripped value.
    cleaned = [job_id.strip() for job_id in job_ids if job_id.strip()]
    if not cleaned:
        raise RpcError("invalid_params", "job_ids must be a non-empty list of strings")
    unique = list(dict.fromkeys(cleaned))
    accepted, skipped = unique[:MAX_JOB_INFO_BATCH], unique[MAX_JOB_INFO_BATCH:]
    # Unlike the row/line clamps elsewhere in this module, silently dropping
    # part of a *set* of ids would look like "the host has no record of those".
    # Name them so the client can re-ask.
    return {"states": lookup_job_states(accepted), "skipped": skipped}


def handle_tail(params: dict) -> dict:
    params = _require_dict(params)
    host = _require_str(params, "host")
    lines = _optional_int(params, "lines")
    if lines is None:
        lines = 200
    # Clamp to a sane range so a phone client can't ask the host to ship a 10MB log over SSH.
    lines = max(1, min(lines, 5000))
    return tail_remote_log(host, lines=lines)


def handle_stats(params: dict) -> dict:
    """Aggregated counters for a phone dashboard. One round trip, no math on the client.

    Combines the queue config (host pool), running state, queued list, and host
    cooldowns. Returns counts plus the underlying name lists so the UI can
    drill in (or render a sparkline of which hosts are busy) without a follow-up.
    """
    _require_dict(params)
    queue_host_map = _load_queue_host_map()
    host_pool = sorted({h for hosts in queue_host_map.values() for h in hosts})

    running = load_running_jobs()
    running_hosts = sorted(running.keys())

    queued = [normalize_job_item(item) for item in load_queue()]
    queued_by_queue: dict[str, int] = {}
    for job in queued:
        queued_by_queue[job.get("queue") or "default"] = queued_by_queue.get(job.get("queue") or "default", 0) + 1
    # Surface configured queues with 0 jobs too, so the UI can show every row even when idle.
    for queue_name in queue_host_map:
        queued_by_queue.setdefault(queue_name, 0)

    cooldown_hosts = sorted(get_host_cooldowns().keys())

    total = len(host_pool)
    running_count = len(running_hosts)
    fraction_empty = (total - running_count) / total if total else 0.0

    return {
        "running_count": running_count,
        "queued_count": len(queued),
        "host_total": total,
        "host_pool": host_pool,
        "running_hosts": running_hosts,
        "cooldown_hosts": cooldown_hosts,
        "queue_host_map": {q: list(hs) for q, hs in queue_host_map.items()},
        "queued_by_queue": queued_by_queue,
        "fraction_empty": fraction_empty,
    }


# ---------- registry + dispatcher ----------

METHODS: dict[str, Callable[[dict], dict]] = {
    "enqueue": handle_enqueue,
    "list": handle_list,
    "qstat": handle_qstat,
    "qdel": handle_qdel,
    "deferred_list": handle_deferred_list,
    "failed_list": handle_failed_list,
    "requeue_deferred": handle_requeue_deferred,
    "list_cooldowns": handle_list_cooldowns,
    "enable_host": handle_enable_host,
    "job_info": handle_job_info,
    "job_info_batch": handle_job_info_batch,
    "tail": handle_tail,
    "stats": handle_stats,
}


def dispatch(request: Any) -> dict:
    """Run one request and return the response envelope dict."""
    if not isinstance(request, dict):
        return make_error("bad_request", "request must be a JSON object")
    if request.get("version") != PROTOCOL_VERSION:
        return make_error("bad_request", f"unsupported version: {request.get('version')!r}")
    method = request.get("method")
    if not isinstance(method, str) or not method:
        return make_error("bad_request", "missing or invalid method")
    params = request.get("params")
    if params is None:
        params = {}
    if not isinstance(params, dict):
        return make_error("bad_request", "params must be an object")
    handler = METHODS.get(method)
    if handler is None:
        return make_error("unknown_method", f"unknown method: {method}")
    try:
        result = handler(params)
        return make_ok(result)
    except RpcError as exc:
        return make_error(exc.code, exc.message)
    except Exception as exc:  # noqa: BLE001 — last-resort serialization for the response envelope
        return make_error("internal", f"{type(exc).__name__}: {exc}")


def run_rpc_stdin_stdout() -> int:
    """Read one request from stdin, write one response to stdout. Returns exit code."""
    raw = sys.stdin.read()
    if not raw.strip():
        response = make_error("bad_request", "empty request body")
    else:
        try:
            request = json.loads(raw)
        except json.JSONDecodeError as exc:
            response = make_error("bad_request", f"invalid JSON: {exc}")
        else:
            response = dispatch(request)
    sys.stdout.write(json.dumps(response))
    sys.stdout.write("\n")
    sys.stdout.flush()
    return 0
