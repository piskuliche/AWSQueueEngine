import json

from .paths import RUNNING_FILE
from .queue import normalize_job_item


def _normalize_started_at(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if value >= 0 else None
    if isinstance(value, str):
        try:
            parsed = float(value.strip())
        except ValueError:
            return None
        return parsed if parsed >= 0 else None
    return None


def normalize_running_item(item):
    normalized_item = normalize_job_item(item)
    started_at = None
    if isinstance(item, dict):
        started_at = _normalize_started_at(item.get("started_at"))
    normalized_item["started_at"] = started_at
    return normalized_item


def load_running_jobs():
    if not RUNNING_FILE.exists():
        return {}
    try:
        data = json.loads(RUNNING_FILE.read_text())
    except Exception:
        return {}

    if not isinstance(data, dict):
        return {}

    normalized = {}
    for host, job in data.items():
        if not isinstance(host, str):
            continue
        clean_host = host.strip()
        if not clean_host:
            continue
        normalized[clean_host] = normalize_running_item(job)
    return normalized


def save_running_jobs(running_jobs):
    RUNNING_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not isinstance(running_jobs, dict):
        RUNNING_FILE.write_text("{}")
        return

    serializable = {}
    for host, job in running_jobs.items():
        if not isinstance(host, str):
            continue
        clean_host = host.strip()
        if not clean_host:
            continue
        serializable[clean_host] = normalize_running_item(job)
    RUNNING_FILE.write_text(json.dumps(serializable, indent=2))
