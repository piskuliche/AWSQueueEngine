import json

from .paths import COMPLETED_FILE
from .state_io import warn_unreadable, write_json_atomic
from .state_lock import state_lock


def load_completed_jobs():
    if not COMPLETED_FILE.exists():
        return []
    try:
        data = json.loads(COMPLETED_FILE.read_text())
    except Exception as exc:
        warn_unreadable(COMPLETED_FILE, exc)
        return []
    return data if isinstance(data, list) else []


def save_completed_jobs(records):
    if not isinstance(records, list):
        write_json_atomic(COMPLETED_FILE, [])
        return
    write_json_atomic(COMPLETED_FILE, records)


def append_completed_records(records):
    if not records:
        return
    with state_lock():
        current = load_completed_jobs()
        current.extend(records)
        save_completed_jobs(current)
