"""Client-side configuration.

Persisted to ``~/.awsqe/client/config.toml`` so the user can set
``queue_host`` (and S3 settings) once instead of passing them on every
invocation. Schema::

    [default]
    queue_host = "queue-manager"

    [s3]
    bucket = "my-queue-payload-bucket"
    prefix = "awsqueueengine/payloads"

Resolution precedence for each setting is **CLI flag > env var > config
file > built-in default (or error if no default)**, applied via the
``effective_*`` helpers.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Python 3.11+ has tomllib in stdlib; 3.10 uses the `tomli` backport.
if sys.version_info >= (3, 11):
    import tomllib as _tomllib
else:  # pragma: no cover — backport path only exercised on 3.10
    import tomli as _tomllib


CONFIG_DIR = Path.home() / ".awsqe" / "client"
CONFIG_PATH = CONFIG_DIR / "config.toml"

DEFAULT_S3_PREFIX = "awsqueueengine/payloads"


# ---------- schema for the `awsqe-client config <set|get|unset>` keys ----------

# User-visible key → (toml_section, toml_field). Hyphens and underscores are
# treated as equivalent on input; the canonical form below is what we write.
KEY_SCHEMA: dict[str, tuple[str, str]] = {
    "queue_host": ("default", "queue_host"),
    "s3.bucket": ("s3", "bucket"),
    "s3.prefix": ("s3", "prefix"),
}


def normalize_key(raw: str) -> str:
    """Accept `queue-host` / `queue_host`, `s3.bucket`, etc. and return the canonical key."""
    if not isinstance(raw, str):
        raise ValueError("config key must be a string")
    normalized = raw.strip().lower().replace("-", "_")
    if normalized in KEY_SCHEMA:
        return normalized
    raise ValueError(
        f"unknown config key: {raw!r}. Known: {', '.join(sorted(KEY_SCHEMA))}"
    )


# ---------- ClientConfig dataclass ----------

@dataclass
class ClientConfig:
    queue_host: str | None = None
    s3_bucket: str | None = None
    s3_prefix: str | None = None  # None means "no override; use built-in default"
    # Anything we don't understand from the file is preserved so we don't drop
    # the user's data on save.
    extra: dict[str, dict[str, Any]] = field(default_factory=dict)

    def as_toml_sections(self) -> dict[str, dict[str, Any]]:
        sections: dict[str, dict[str, Any]] = {}
        if self.queue_host:
            sections.setdefault("default", {})["queue_host"] = self.queue_host
        if self.s3_bucket or self.s3_prefix:
            s3 = sections.setdefault("s3", {})
            if self.s3_bucket:
                s3["bucket"] = self.s3_bucket
            if self.s3_prefix:
                s3["prefix"] = self.s3_prefix
        # Preserve any unknown sections/keys the user added by hand.
        for section, table in self.extra.items():
            existing = sections.setdefault(section, {})
            for k, v in table.items():
                existing.setdefault(k, v)
        return sections


# ---------- load / save ----------

def load_config(path: Path | None = None) -> ClientConfig:
    """Load the client config. Missing file is treated as an empty config."""
    target = path or CONFIG_PATH
    if not target.exists():
        return ClientConfig()
    try:
        with open(target, "rb") as f:
            data = _tomllib.load(f)
    except Exception as exc:
        print(f"[WARN] failed to parse {target}: {exc}", file=sys.stderr, flush=True)
        return ClientConfig()
    if not isinstance(data, dict):
        return ClientConfig()
    config = ClientConfig()
    known_pairs = set(KEY_SCHEMA.values())
    for section, table in data.items():
        if not isinstance(table, dict):
            continue
        for key, value in table.items():
            pair = (section, key)
            consumed = False
            if pair == ("default", "queue_host") and isinstance(value, str):
                config.queue_host = value.strip() or None
                consumed = True
            elif pair == ("s3", "bucket") and isinstance(value, str):
                config.s3_bucket = value.strip() or None
                consumed = True
            elif pair == ("s3", "prefix") and isinstance(value, str):
                config.s3_prefix = value.strip().strip("/") or None
                consumed = True
            if consumed:
                continue
            # A known key with the wrong type would otherwise be silently
            # dropped on the next save. Stash it in `extra` so save_config
            # round-trips it and warn so the user knows to clean it up.
            if pair in known_pairs:
                print(
                    f"[WARN] config: {section}.{key} has wrong type "
                    f"({type(value).__name__}); preserving as-is and ignoring it.",
                    file=sys.stderr,
                    flush=True,
                )
            config.extra.setdefault(section, {})[key] = value
    return config


def save_config(config: ClientConfig, path: Path | None = None) -> Path:
    """Atomically write the client config to disk. Creates parents as needed."""
    target = path or CONFIG_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    text = _render_toml(config.as_toml_sections())
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, target)
    return target


def _render_toml(sections: dict[str, dict[str, Any]]) -> str:
    """Tiny TOML emitter for our two-section schema. Strings only."""
    lines: list[str] = []
    # Stable order: default first, then s3, then any extras alphabetized.
    ordered = []
    for preferred in ("default", "s3"):
        if preferred in sections:
            ordered.append(preferred)
    for extra in sorted(k for k in sections if k not in ("default", "s3")):
        ordered.append(extra)
    for section in ordered:
        table = sections[section]
        if not table:
            continue
        if lines:
            lines.append("")
        lines.append(f"[{section}]")
        for key in sorted(table):
            value = table[key]
            lines.append(f"{key} = {_render_toml_value(value)}")
    return "\n".join(lines) + "\n" if lines else ""


def _render_toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    # Strings AND any non-numeric extra value (e.g. things tomllib gave us as
    # lists/dicts under `extra`) get coerced to text and routed through the
    # same TOML basic-string escape, so the output is always valid TOML on
    # save — never invalid syntax just because str(value) happened to contain
    # a quote or backslash.
    text = value if isinstance(value, str) else str(value)
    escaped = (
        text.replace("\\", "\\\\")
            .replace('"', '\\"')
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
    )
    return f'"{escaped}"'


# ---------- precedence resolution ----------

def effective_queue_host(cli_value: str | None, config: ClientConfig | None = None) -> str | None:
    """CLI flag > config > None. No env var defined for this setting."""
    if cli_value:
        return cli_value
    if config is None:
        config = load_config()
    return config.queue_host or None


def effective_s3_bucket(config: ClientConfig | None = None) -> str | None:
    """Env var (AWSQUEUEENGINE_S3_BUCKET) > config > None."""
    env_value = os.getenv("AWSQUEUEENGINE_S3_BUCKET", "").strip()
    if env_value:
        return env_value
    if config is None:
        config = load_config()
    return config.s3_bucket or None


def effective_s3_prefix(config: ClientConfig | None = None) -> str:
    """Env var (AWSQUEUEENGINE_S3_PREFIX) > config > built-in default."""
    env_value = os.getenv("AWSQUEUEENGINE_S3_PREFIX", "").strip().strip("/")
    if env_value:
        return env_value
    if config is None:
        config = load_config()
    return config.s3_prefix or DEFAULT_S3_PREFIX


# ---------- set/get/unset (used by the CLI) ----------

def set_value(config: ClientConfig, canonical_key: str, value: str) -> None:
    if canonical_key == "queue_host":
        config.queue_host = value.strip() or None
    elif canonical_key == "s3.bucket":
        config.s3_bucket = value.strip() or None
    elif canonical_key == "s3.prefix":
        config.s3_prefix = value.strip().strip("/") or None
    else:
        raise ValueError(f"unknown config key: {canonical_key!r}")


def unset_value(config: ClientConfig, canonical_key: str) -> None:
    if canonical_key == "queue_host":
        config.queue_host = None
    elif canonical_key == "s3.bucket":
        config.s3_bucket = None
    elif canonical_key == "s3.prefix":
        config.s3_prefix = None
    else:
        raise ValueError(f"unknown config key: {canonical_key!r}")


def get_value(config: ClientConfig, canonical_key: str) -> str | None:
    if canonical_key == "queue_host":
        return config.queue_host
    if canonical_key == "s3.bucket":
        return config.s3_bucket
    if canonical_key == "s3.prefix":
        return config.s3_prefix
    raise ValueError(f"unknown config key: {canonical_key!r}")
