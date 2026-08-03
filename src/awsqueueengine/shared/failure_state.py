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
from .state_io import warn_unreadable, write_json_atomic

MAX_FAILED_RECORDS = 1000


def load_failed_jobs():
    if not FAILED_FILE.exists():
        return []
    try:
        data = json.loads(FAILED_FILE.read_text())
    except Exception as exc:
        warn_unreadable(FAILED_FILE, exc)
        return []
    return data if isinstance(data, list) else []


def save_failed_jobs(records):
    if not isinstance(records, list):
        write_json_atomic(FAILED_FILE, [])
        return
    write_json_atomic(FAILED_FILE, records[-MAX_FAILED_RECORDS:])


def append_failed_records(records):
    if not records:
        return
    current = load_failed_jobs()
    current.extend(records)
    save_failed_jobs(current)
