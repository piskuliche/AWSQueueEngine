"""Queue-host configuration: where the host learns which workers serve which queues.

Discovery precedence used by :func:`get_configured_queue_source`:

1. ``AWSQUEUEENGINE_QUEUES_FILE`` — explicit path to a queues JSON file.
2. ``AWSQUEUEENGINE_QUEUES`` — inline config in the env var itself.
3. ``~/.awsqe/host/queues.json`` — canonical disk location (preferred).
4. ``~/awsqueueengine_queues.json`` — legacy home-dir location (backward compat).
5. Built-in ``HOSTS`` constant — single ``default`` queue with ``eci1..eci20``.

Steps 3 and 4 exist because the daemon is typically started via systemd with
the env var set, but ad-hoc ``awsqe-host rpc`` invocations over non-interactive
SSH don't inherit that env. Without a disk fallback, daemon and RPC subprocess
would disagree about the queue config, and clients (Android viewer, desktop
``awsqe-client`` on a third machine, etc.) would silently see the hardcoded
defaults instead of the real config.

(1) and (2) are mutually exclusive — setting both raises ``ValueError``.
"""
import json
import os
from pathlib import Path

from .config import HOSTS
from .paths import HOST_STATE_DIR


DEFAULT_QUEUE = "default"
QUEUES_FILE_ENV = "AWSQUEUEENGINE_QUEUES_FILE"
QUEUES_ENV = "AWSQUEUEENGINE_QUEUES"

# Default discovery paths used when neither QUEUES_FILE_ENV nor QUEUES_ENV is
# set. Searched in order; first existing file wins. This exists so that the
# daemon (started via systemd with the env var) and ad-hoc `awsqe-host rpc`
# invocations over non-interactive SSH (where the env var is usually absent)
# converge on the same queue config without operator action.
DEFAULT_QUEUES_CONFIG_PATHS = (
    HOST_STATE_DIR / "queues.json",
    Path.home() / "awsqueueengine_queues.json",
)


def normalize_queue_name(value):
    if not isinstance(value, str):
        return DEFAULT_QUEUE
    clean = value.strip()
    if not clean:
        return DEFAULT_QUEUE
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in clean).strip("._-") or DEFAULT_QUEUE


def normalize_hosts(host_values):
    normalized_hosts = []
    seen = set()
    for host in host_values or []:
        if not isinstance(host, str):
            continue
        clean_host = host.strip()
        if not clean_host or clean_host in seen:
            continue
        normalized_hosts.append(clean_host)
        seen.add(clean_host)
    return normalized_hosts


def parse_hosts_text(hosts_text):
    return normalize_hosts(hosts_text.replace(",", " ").split())


def load_hosts_from_file(hosts_file):
    hosts_path = Path(hosts_file).expanduser()
    raw_text = hosts_path.read_text()
    parsed_hosts = []
    for raw_line in raw_text.splitlines():
        line = raw_line.split("#", 1)[0].replace(",", " ")
        parsed_hosts.extend(line.split())
    return normalize_hosts(parsed_hosts)


def _coerce_hosts(value):
    if isinstance(value, str):
        return parse_hosts_text(value)
    if isinstance(value, (list, tuple, set)):
        return normalize_hosts(value)
    return []


def _normalize_queue_config(raw_config):
    if not isinstance(raw_config, dict):
        raise ValueError("queue config must be a mapping of queue names to host lists")
    queues = {}
    for raw_name, raw_hosts in raw_config.items():
        queue_name = normalize_queue_name(str(raw_name))
        hosts = _coerce_hosts(raw_hosts)
        if hosts:
            queues[queue_name] = hosts
    if not queues:
        raise ValueError("queue config does not define any hosts")
    return queues


def load_queue_config_from_file(queues_file):
    config_path = Path(queues_file).expanduser()
    raw_text = config_path.read_text()
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{config_path} must be JSON, for example {{\"default\": [\"eci1\"]}}") from exc
    return _normalize_queue_config(parsed)


def load_queue_config_from_env(queues_text):
    raw = {}
    for entry in queues_text.replace("\n", ";").split(";"):
        entry = entry.strip()
        if not entry:
            continue
        if "=" in entry:
            name, hosts_text = entry.split("=", 1)
        else:
            name, hosts_text = DEFAULT_QUEUE, entry
        raw[normalize_queue_name(name)] = parse_hosts_text(hosts_text)
    return _normalize_queue_config(raw)


def _queue_name_from_legacy_suffix(suffix):
    return normalize_queue_name(suffix.lower().replace("_", "-"))


def _load_legacy_host_set_queues():
    queues = {}
    inline_prefix = "AWSQUEUEENGINE_HOST_SET_"
    file_prefix = "AWSQUEUEENGINE_HOSTS_FILE_"
    for key, value in os.environ.items():
        if key.startswith(inline_prefix) and value.strip():
            queue_name = _queue_name_from_legacy_suffix(key[len(inline_prefix):])
            hosts = parse_hosts_text(value)
            if hosts:
                queues[queue_name] = hosts
        elif key.startswith(file_prefix) and value.strip():
            queue_name = _queue_name_from_legacy_suffix(key[len(file_prefix):])
            try:
                hosts = load_hosts_from_file(value)
            except OSError:
                continue
            if hosts:
                queues[queue_name] = hosts
    return queues


def get_configured_queue_source():
    queues_file = os.getenv(QUEUES_FILE_ENV, "").strip()
    queues_env = os.getenv(QUEUES_ENV, "").strip()
    if queues_file and queues_env:
        raise ValueError(f"{QUEUES_FILE_ENV} and {QUEUES_ENV} cannot both be set")
    if queues_file:
        return "file", queues_file
    if queues_env:
        return "env", queues_env
    # Neither env var set — try canonical disk paths so the daemon and the
    # ad-hoc RPC subprocess (which may not inherit the env) agree.
    for path in DEFAULT_QUEUES_CONFIG_PATHS:
        try:
            if path.is_file():
                return "file", str(path)
        except OSError:
            continue
    return None, None


class QueueConfigSource:
    def __init__(self, legacy_hosts=None, legacy_hosts_file=None):
        source_kind, source_value = get_configured_queue_source()
        self._source_kind = source_kind
        self._source_value = source_value
        self._legacy_hosts = normalize_hosts(legacy_hosts or HOSTS)
        self._legacy_hosts_file = Path(legacy_hosts_file).expanduser() if legacy_hosts_file else None
        self._last_file_stamp = None
        self._missing_file_warned = False
        self._queues = {}
        self.refresh(force=True)

    @property
    def source_kind(self):
        return self._source_kind or ("file" if self._legacy_hosts_file else "legacy")

    @property
    def source_value(self):
        return self._source_value or (str(self._legacy_hosts_file) if self._legacy_hosts_file else "built-in default")

    def _active_file(self):
        if self._source_kind == "file":
            return Path(self._source_value).expanduser()
        if not self._source_kind and self._legacy_hosts_file:
            return self._legacy_hosts_file
        return None

    def _compute_file_stamp(self, path):
        try:
            stat_result = path.stat()
        except FileNotFoundError:
            return None
        return (stat_result.st_mtime_ns, stat_result.st_size)

    def _load_from_active_file(self, path):
        if self._source_kind == "file":
            return load_queue_config_from_file(path)
        return {DEFAULT_QUEUE: load_hosts_from_file(path)}

    def refresh(self, force=False):
        if self._source_kind == "env":
            if force or not self._queues:
                self._queues = load_queue_config_from_env(self._source_value)
            return dict(self._queues)

        active_file = self._active_file()
        if active_file:
            file_stamp = self._compute_file_stamp(active_file)
            if file_stamp is None:
                if not self._missing_file_warned:
                    print(
                        f"[WARN] Queue hosts file not found: {active_file}. "
                        f"Keeping previous queue config ({len(self.all_hosts())} host(s)).",
                        flush=True,
                    )
                    self._missing_file_warned = True
                self._last_file_stamp = None
                return dict(self._queues)

            if not force and self._last_file_stamp == file_stamp:
                return dict(self._queues)

            try:
                next_queues = self._load_from_active_file(active_file)
            except (OSError, ValueError) as exc:
                if force and not self._queues:
                    raise
                print(
                    f"[WARN] Failed to read queue hosts file {active_file}: {exc}. "
                    f"Keeping previous queue config ({len(self.all_hosts())} host(s)).",
                    flush=True,
                )
                self._last_file_stamp = file_stamp
                return dict(self._queues)
            self._missing_file_warned = False
            if next_queues != self._queues:
                print(
                    f"[INFO] Queue hosts updated from {active_file}: "
                    f"{len(next_queues)} queue(s), {len(_all_hosts(next_queues))} host(s)",
                    flush=True,
                )
                self._queues = next_queues
            self._last_file_stamp = file_stamp
            return dict(self._queues)

        self._queues = {DEFAULT_QUEUE: list(self._legacy_hosts)}
        self._queues.update(_load_legacy_host_set_queues())
        return dict(self._queues)

    def all_hosts(self):
        return _all_hosts(self._queues)


def _all_hosts(queue_map):
    hosts = []
    seen = set()
    for queue_hosts in queue_map.values():
        for host in queue_hosts:
            if host not in seen:
                hosts.append(host)
                seen.add(host)
    return hosts


def queue_hosts_for_item(item, queue_map):
    legacy_hosts = item.get("hosts") if isinstance(item, dict) else None
    if legacy_hosts:
        return normalize_hosts(legacy_hosts)
    queue_name = normalize_queue_name(item.get("queue") if isinstance(item, dict) else DEFAULT_QUEUE)
    return list(queue_map.get(queue_name, []))


def host_is_eligible_for_item(item, host, queue_map=None):
    if not isinstance(item, dict):
        return True
    legacy_hosts = item.get("hosts")
    if legacy_hosts:
        return host in normalize_hosts(legacy_hosts)
    if queue_map is None:
        return True
    queue_name = normalize_queue_name(item.get("queue", DEFAULT_QUEUE))
    return host in queue_map.get(queue_name, [])
