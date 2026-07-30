"""Locate a job by job_id across the queue / running / completed / failed state files.

Used both by the host's `job-info` RPC handler (called over SSH from the
client) and by direct local reads when client + host are the same machine.
"""
from .completion_state import load_completed_jobs
from .failure_state import load_failed_jobs
from .queue import load_queue, normalize_job_item
from .run_info import format_epoch
from .running_state import load_running_jobs


def lookup_job_state(job_id):
    if not job_id:
        return None
    for idx, raw_item in enumerate(load_queue(), 1):
        item = normalize_job_item(raw_item)
        if item.get("job_id") == job_id:
            hosts = item.get("hosts")
            return {
                "status": "queued",
                "job_id": job_id,
                "queue_position": idx,
                "queue": item.get("queue"),
                "hosts_filter": ",".join(hosts) if hosts else "",
                "cmd": str(item.get("cmd") or ""),
            }
    running_jobs = load_running_jobs()
    for host, raw_item in running_jobs.items():
        item = normalize_job_item(raw_item)
        if item.get("job_id") == job_id:
            started_at = raw_item.get("started_at") if isinstance(raw_item, dict) else None
            return {
                "status": "running",
                "job_id": job_id,
                "host": host,
                "remote_payload_path": item.get("payload_remote_path") or "",
                "started_at": format_epoch(started_at),
                "queue": item.get("queue"),
                "cmd": str(item.get("cmd") or ""),
            }
    return _lookup_finished_job(job_id)


def describe_missing_job(job_id):
    """Where a job id went, for one that `qdel` found no queue entry for.

    Returns ``None`` when the id is unknown or still queued (in which case the
    caller's own "no queued job matching" message is already the whole story).
    """
    state = lookup_job_state(job_id)
    if not state:
        return None
    status = state.get("status")
    if status == "queued":
        return None
    if status == "running":
        host = state.get("host") or "?"
        return f"{job_id} is already running on {host}; qdel only removes queued jobs (use `stop`)"
    return f"{job_id} already finished ({status})"


def enrich_selection_message(message, tokens):
    """Append per-token context to a qdel "not found" message, when we have any."""
    notes = [note for note in (describe_missing_job(t) for t in tokens or []) if note]
    if not notes:
        return message
    return f"{message} ({'; '.join(notes)})"


def _find_last_record(records, job_id):
    for record in reversed(records):
        if isinstance(record, dict) and record.get("job_id") == job_id:
            return record
    return None


def _finished_at_of(record):
    value = record.get("finished_at") if isinstance(record, dict) else None
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else 0.0


def _lookup_finished_job(job_id):
    """Most recent terminal record for the job, from either history file.

    A requeued job can appear in both histories (failed once, completed on the
    retry), so the newer ``finished_at`` wins.
    """
    completed = _find_last_record(load_completed_jobs(), job_id)
    failed = _find_last_record(load_failed_jobs(), job_id)
    if completed is None and failed is None:
        return None
    if failed is not None and (completed is None or _finished_at_of(failed) >= _finished_at_of(completed)):
        record, status = failed, "failed"
    else:
        # "unknown" for jobs that finished without a recorded exit status
        # (launched before the monitor started tracking it).
        record = completed
        status = record.get("status") if record.get("status") in {"completed", "unknown"} else "completed"

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
    if status == "failed":
        exit_code = record.get("exit_code")
        state["failure_reason"] = record.get("failure_reason") or "unknown"
        state["failure_detail"] = record.get("failure_detail") or ""
        state["exit_code"] = "" if exit_code is None else str(exit_code)
    return state
