"""run.info file IO — used by both client and host paths.

Format is one key:value pair per line. Used to track the bridge between
a local payload directory on the submitter and the queue host's view of
the job (job_id, queue host, host, remote payload, status, timestamps).
"""
from datetime import datetime
from pathlib import Path


def write_local_run_info(payload_path, info):
    """Write a run.info file in a local payload dir with submit metadata."""
    if not payload_path:
        return None
    target_dir = Path(payload_path).expanduser()
    if not target_dir.exists() or not target_dir.is_dir():
        return None
    info_path = target_dir / "run.info"
    lines = []
    for key, value in info.items():
        if value is None or value == "":
            continue
        lines.append(f"{key}: {value}")
    try:
        info_path.write_text("\n".join(lines) + "\n")
    except OSError as exc:
        print(f"[WARN] Could not write {info_path}: {exc}", flush=True)
        return None
    return info_path


def read_run_info_file(info_path):
    info = {}
    for line in info_path.read_text().splitlines():
        if ":" not in line:
            continue
        key, _, value = line.partition(":")
        clean_key = key.strip()
        if not clean_key:
            continue
        info[clean_key] = value.strip()
    return info


def write_run_info_file(info_path, info):
    lines = []
    for key, value in info.items():
        if value is None or value == "":
            continue
        lines.append(f"{key}: {value}")
    info_path.write_text("\n".join(lines) + "\n")


def format_epoch(value):
    if isinstance(value, bool):
        return ""
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value)).strftime("%Y-%m-%d %H:%M:%S")
        except (OverflowError, OSError, ValueError):
            return ""
    return ""
