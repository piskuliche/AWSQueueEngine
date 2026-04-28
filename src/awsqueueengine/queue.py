# Queue management helpers
import json
from .config import QUEUE_FILE

DEFAULT_PRIORITY = 0
DEFAULT_PREEMPT = False
DEFAULT_RESUME_FIRST = False
LEGACY_PRIORITY_MAP = {
    "high": 100,
    "normal": 0,
}


def load_queue():
    if QUEUE_FILE.exists():
        try:
            data = json.loads(QUEUE_FILE.read_text())
            return data if isinstance(data, list) else []
        except Exception:
            return []
    return []


def save_queue(q):
    QUEUE_FILE.write_text(json.dumps(q, indent=2))


def _normalize_priority(value):
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        priority_text = value.strip().lower()
        if priority_text in LEGACY_PRIORITY_MAP:
            return LEGACY_PRIORITY_MAP[priority_text]
        try:
            return int(priority_text)
        except ValueError:
            return DEFAULT_PRIORITY
    return DEFAULT_PRIORITY


def _normalize_hosts(hosts):
    if hosts is None:
        return None
    if isinstance(hosts, str):
        host_values = [hosts]
    elif isinstance(hosts, (list, tuple, set)):
        host_values = list(hosts)
    else:
        return None

    normalized_hosts = []
    seen = set()
    for host in host_values:
        if not isinstance(host, str):
            continue
        clean_host = host.strip()
        if not clean_host or clean_host in seen:
            continue
        normalized_hosts.append(clean_host)
        seen.add(clean_host)
    return normalized_hosts or None


def _normalize_preempt(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return DEFAULT_PREEMPT


def _normalize_payload_remote_path(value):
    if not isinstance(value, str):
        return None
    clean_value = value.strip()
    return clean_value or None


def _normalize_payload_s3_uri(value):
    if not isinstance(value, str):
        return None
    clean_value = value.strip()
    return clean_value or None


def _normalize_payload_size_bytes(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str):
        try:
            parsed = int(value.strip())
        except ValueError:
            return None
        return parsed if parsed >= 0 else None
    return None


def _normalize_resume_first(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return DEFAULT_RESUME_FIRST


def _normalize_resume_host(value):
    if not isinstance(value, str):
        return None
    clean_value = value.strip()
    return clean_value or None


def normalize_job_item(item):
    if isinstance(item, str):
        return {
            "cmd": item,
            "payload": None,
            "priority": DEFAULT_PRIORITY,
            "hosts": None,
            "preempt": DEFAULT_PREEMPT,
            "payload_remote_path": None,
            "payload_s3_uri": None,
            "payload_size_bytes": None,
            "resume_first": DEFAULT_RESUME_FIRST,
            "resume_host": None,
        }

    if not isinstance(item, dict):
        return {
            "cmd": str(item),
            "payload": None,
            "priority": DEFAULT_PRIORITY,
            "hosts": None,
            "preempt": DEFAULT_PREEMPT,
            "payload_remote_path": None,
            "payload_s3_uri": None,
            "payload_size_bytes": None,
            "resume_first": DEFAULT_RESUME_FIRST,
            "resume_host": None,
        }

    submit_failures = item.get("submit_failures", 0)
    if not isinstance(submit_failures, int) or submit_failures < 0:
        submit_failures = 0
    return {
        "cmd": item.get("cmd"),
        "payload": item.get("payload"),
        "priority": _normalize_priority(item.get("priority", DEFAULT_PRIORITY)),
        "hosts": _normalize_hosts(item.get("hosts")),
        "preempt": _normalize_preempt(item.get("preempt", DEFAULT_PREEMPT)),
        "payload_remote_path": _normalize_payload_remote_path(item.get("payload_remote_path")),
        "payload_s3_uri": _normalize_payload_s3_uri(item.get("payload_s3_uri")),
        "payload_size_bytes": _normalize_payload_size_bytes(item.get("payload_size_bytes")),
        "resume_first": _normalize_resume_first(item.get("resume_first", DEFAULT_RESUME_FIRST)),
        "resume_host": _normalize_resume_host(item.get("resume_host")),
        "submit_failures": submit_failures,
    }


def build_resume_item(job_item, host, priority=None):
    item = normalize_job_item(job_item)
    item["hosts"] = [host]
    item["resume_first"] = True
    item["resume_host"] = host
    item["submit_failures"] = 0
    if priority is not None:
        item["priority"] = _normalize_priority(priority)
    return item


def enqueue_item(item):
    q = load_queue()
    q.append(normalize_job_item(item))
    save_queue(q)


def _is_host_eligible(item, host):
    hosts = item.get("hosts")
    return not hosts or host in hosts


def _select_best_index(q, host=None):
    if host is not None:
        for idx, queued_item in enumerate(q):
            normalized_item = normalize_job_item(queued_item)
            if not _is_host_eligible(normalized_item, host):
                continue
            if normalized_item.get("resume_first") and normalized_item.get("resume_host") == host:
                return idx

    best_idx = None
    best_priority = None

    for idx, queued_item in enumerate(q):
        normalized_item = normalize_job_item(queued_item)
        if host is not None and not _is_host_eligible(normalized_item, host):
            continue

        priority = normalized_item["priority"]
        if best_idx is None or priority > best_priority:
            best_idx = idx
            best_priority = priority
    return best_idx


def _dequeue_index(q, idx):
    if idx is None:
        return None
    item = normalize_job_item(q.pop(idx))
    save_queue(q)
    return item


def dequeue():
    """Dequeue the highest-priority job (FIFO tie-breaker)."""
    q = load_queue()
    if not q:
        return None
    return _dequeue_index(q, _select_best_index(q))


def dequeue_for_host(host):
    """Dequeue the highest-priority job eligible for the provided host."""
    q = load_queue()
    if not q:
        return None
    return _dequeue_index(q, _select_best_index(q, host=host))
