"""Persistent history of jobs that failed.

Mirrors :mod:`completion_state`, but for the jobs that did *not* finish
cleanly: a nonzero exit status on the worker, a job that died before it
ever recorded one, or a job that never managed to start at all. Records
land in ``~/.awsqe/host/failed.json``.

The list is capped at :data:`MAX_FAILED_RECORDS` (oldest dropped first) so a
host that is failing every job cannot grow the file without bound.
"""
import json

from .paths import FAILED_FILE

MAX_FAILED_RECORDS = 1000


def load_failed_jobs():
    if not FAILED_FILE.exists():
        return []
    try:
        data = json.loads(FAILED_FILE.read_text())
    except Exception:
        return []
    return data if isinstance(data, list) else []


def save_failed_jobs(records):
    FAILED_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not isinstance(records, list):
        FAILED_FILE.write_text("[]")
        return
    FAILED_FILE.write_text(json.dumps(records[-MAX_FAILED_RECORDS:], indent=2))


def append_failed_records(records):
    if not records:
        return
    current = load_failed_jobs()
    current.extend(records)
    save_failed_jobs(current)
