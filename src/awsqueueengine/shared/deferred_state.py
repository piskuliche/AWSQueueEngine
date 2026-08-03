import json
import time

from .paths import DEFERRED_FILE
from .queue import normalize_job_item
from .state_io import warn_unreadable, write_json_atomic


def load_deferred_jobs():
    if not DEFERRED_FILE.exists():
        return []
    try:
        data = json.loads(DEFERRED_FILE.read_text())
        return data if isinstance(data, list) else []
    except Exception as exc:
        warn_unreadable(DEFERRED_FILE, exc)
        return []


def save_deferred_jobs(jobs):
    write_json_atomic(DEFERRED_FILE, jobs)


def append_deferred_job(job_item, last_error=None, last_host=None):
    jobs = load_deferred_jobs()
    item = normalize_job_item(job_item)
    item["submit_failures"] = 0
    record = {
        **item,
        "deferred_at": time.time(),
        "last_error": last_error or "",
        "last_host": last_host or "",
    }
    jobs.append(record)
    save_deferred_jobs(jobs)
    return record


def pop_deferred_by_indices(indices):
    """Pop deferred jobs by 1-based index. Returns list of (idx, record) in original order."""
    jobs = load_deferred_jobs()
    unique = sorted(set(indices))
    popped = []
    for idx in sorted(unique, reverse=True):
        if 1 <= idx <= len(jobs):
            popped.append((idx, jobs.pop(idx - 1)))
    save_deferred_jobs(jobs)
    return sorted(popped, key=lambda pair: pair[0])


def pop_all_deferred():
    jobs = load_deferred_jobs()
    save_deferred_jobs([])
    return list(enumerate(jobs, 1))
