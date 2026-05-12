"""Locate a job by job_id across the queue / running / completed state files.

Used both by the host's `job-info` RPC handler (called over SSH from the
client) and by direct local reads when client + host are the same machine.
"""
from .completion_state import load_completed_jobs
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
    for record in reversed(load_completed_jobs()):
        if isinstance(record, dict) and record.get("job_id") == job_id:
            return {
                "status": "completed",
                "job_id": job_id,
                "host": record.get("host") or "",
                "remote_payload_path": record.get("payload_remote_path") or "",
                "started_at": format_epoch(record.get("started_at")),
                "finished_at": format_epoch(record.get("finished_at")),
                "duration": record.get("dur") or "",
                "queue": record.get("queue") or "",
                "cmd": str(record.get("cmd") or ""),
            }
    return None
