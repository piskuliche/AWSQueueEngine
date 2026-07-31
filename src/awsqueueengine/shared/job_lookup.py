"""Locate a job by job_id across the queue / running / completed / failed state files.

Used both by the host's `job-info` RPC handler (called over SSH from the
client) and by direct local reads when client + host are the same machine.

Two entry points, which must always agree: `lookup_job_state` for one id, and
`lookup_job_states` for many. The batch form exists because the client's
tracked-job ledger asks about hundreds of ids at once, and resolving those one
at a time would re-read (and linearly re-scan) every state file per id.
"""
from .completion_state import load_completed_jobs
from .failure_state import load_failed_jobs
from .job_status import COMPLETED, FAILED, QUEUED, RUNNING, UNKNOWN
from .queue import load_queue, normalize_job_item
from .run_info import format_epoch
from .running_state import load_running_jobs


def lookup_job_state(job_id):
    if not job_id:
        return None
    return lookup_job_states([job_id]).get(job_id)


def lookup_job_states(job_ids):
    """Resolve many job ids with a single read of each state file.

    Returns ``{job_id: state_dict_or_None}``, with an entry for every non-empty
    id asked about, so a caller can tell "not found" (explicit ``None``) from
    "not answered" (key absent). Semantically identical to calling
    :func:`lookup_job_state` per id.
    """
    wanted = [job_id for job_id in dict.fromkeys(job_ids or []) if job_id]
    if not wanted:
        return {}
    states = dict.fromkeys(wanted)
    remaining = set(wanted)

    for idx, raw_item in enumerate(load_queue(), 1):
        if not remaining:
            break
        item = normalize_job_item(raw_item)
        job_id = item.get("job_id")
        if job_id in remaining:
            states[job_id] = _queued_state(job_id, item, idx)
            remaining.discard(job_id)

    if remaining:
        for host, raw_item in load_running_jobs().items():
            if not remaining:
                break
            item = normalize_job_item(raw_item)
            job_id = item.get("job_id")
            if job_id in remaining:
                states[job_id] = _running_state(job_id, item, host, raw_item)
                remaining.discard(job_id)

    # Only touch the history files if something is still unaccounted for. This
    # short-circuit is what keeps qdel's describe_missing_job cheap.
    if remaining:
        completed_index = _index_last_records(load_completed_jobs())
        failed_index = _index_last_records(load_failed_jobs())
        for job_id in remaining:
            states[job_id] = _finished_state(job_id, completed_index, failed_index)

    return states


def describe_missing_job(job_id):
    """Where a job id went, for one that `qdel` found no queue entry for.

    Returns ``None`` when the id is unknown or still queued (in which case the
    caller's own "no queued job matching" message is already the whole story).
    """
    state = lookup_job_state(job_id)
    if not state:
        return None
    status = state.get("status")
    if status == QUEUED:
        return None
    if status == RUNNING:
        host = state.get("host") or "?"
        return f"{job_id} is already running on {host}; qdel only removes queued jobs (use `stop`)"
    return f"{job_id} already finished ({status})"


def enrich_selection_message(message, tokens):
    """Append per-token context to a qdel "not found" message, when we have any."""
    notes = [note for note in (describe_missing_job(t) for t in tokens or []) if note]
    if not notes:
        return message
    return f"{message} ({'; '.join(notes)})"


# ---------- per-status state builders ----------

def _queued_state(job_id, item, position):
    hosts = item.get("hosts")
    return {
        "status": QUEUED,
        "job_id": job_id,
        "queue_position": position,
        "queue": item.get("queue"),
        "hosts_filter": ",".join(hosts) if hosts else "",
        "cmd": str(item.get("cmd") or ""),
    }


def _running_state(job_id, item, host, raw_item):
    started_at = raw_item.get("started_at") if isinstance(raw_item, dict) else None
    return {
        "status": RUNNING,
        "job_id": job_id,
        "host": host,
        "remote_payload_path": item.get("payload_remote_path") or "",
        "started_at": format_epoch(started_at),
        "queue": item.get("queue"),
        "cmd": str(item.get("cmd") or ""),
    }


def _index_last_records(records):
    """job_id -> its last record, in one forward pass.

    Equivalent to reverse-scanning for the first hit, but built once for the
    whole batch instead of re-scanned per id.
    """
    index = {}
    for record in records:
        if isinstance(record, dict) and record.get("job_id"):
            index[record["job_id"]] = record
    return index


def _finished_state(job_id, completed_index, failed_index):
    """Most recent terminal record for the job, from either history file.

    A requeued job can appear in both histories (failed once, completed on the
    retry), so the newer ``finished_at`` wins.
    """
    completed = completed_index.get(job_id)
    failed = failed_index.get(job_id)
    if completed is None and failed is None:
        return None
    if failed is not None and (completed is None or _finished_at_of(failed) >= _finished_at_of(completed)):
        record, status = failed, FAILED
    else:
        # "unknown" for jobs that finished without a recorded exit status
        # (launched before the monitor started tracking it).
        record = completed
        status = record.get("status") if record.get("status") in {COMPLETED, UNKNOWN} else COMPLETED

    state = {
        "status": status,
        "job_id": job_id,
        "host": record.get("host") or "",
        "remote_payload_path": record.get("payload_remote_path") or "",
        "started_at": format_epoch(record.get("started_at")),
        "finished_at": format_epoch(record.get("finished_at")),
        "duration": record.get("dur") or "",
        "queue": record.get("queue") or "",
        "cmd": str(record.get("cmd") or ""),
    }
    if status == FAILED:
        exit_code = record.get("exit_code")
        state["failure_reason"] = record.get("failure_reason") or "unknown"
        state["failure_detail"] = record.get("failure_detail") or ""
        state["exit_code"] = "" if exit_code is None else str(exit_code)
    return state


def _finished_at_of(record):
    value = record.get("finished_at") if isinstance(record, dict) else None
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else 0.0
