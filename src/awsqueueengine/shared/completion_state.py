import json

from .paths import COMPLETED_FILE


def load_completed_jobs():
    if not COMPLETED_FILE.exists():
        return []
    try:
        data = json.loads(COMPLETED_FILE.read_text())
    except Exception:
        return []
    return data if isinstance(data, list) else []


def save_completed_jobs(records):
    COMPLETED_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not isinstance(records, list):
        COMPLETED_FILE.write_text("[]")
        return
    COMPLETED_FILE.write_text(json.dumps(records, indent=2))


def append_completed_records(records):
    if not records:
        return
    current = load_completed_jobs()
    current.extend(records)
    save_completed_jobs(current)
