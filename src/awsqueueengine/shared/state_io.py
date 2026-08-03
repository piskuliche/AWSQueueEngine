"""How the on-disk state files are written, and how an unreadable one is reported.

A bare ``path.write_text(...)`` truncates the file and then refills it, so
there is a window in which the file on disk is a *prefix* of the intended
content. Every state-file reader in this package swallows a parse error and
returns empty, which makes that window indistinguishable from "the queue is
genuinely empty" — `list` reports nothing queued, `job_info` returns ``null``.

Writing a temp file and then :func:`os.replace`-ing it over the target closes
that window: ``rename(2)`` is atomic within a filesystem, so a concurrent reader
sees either the whole old file or the whole new one, never a splice of the two.
It also makes a write that dies partway through a no-op rather than a
truncation.

This does *not* make concurrent read-modify-write safe. Two writers can still
both read, both modify, and have the second replace win — that needs a lock,
not atomicity. See :mod:`awsqueueengine.client.ledger`, which pairs the two.

Callers pass the destination in rather than this module importing it, because
the path constants are module-level names the test suite rebinds; resolving
them here would defeat that.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def temp_path_for(path):
    """The scratch path :func:`write_json_atomic` writes through.

    ``.tmp.<pid>`` rather than a shared ``.tmp`` so a scratch file abandoned by
    a killed process can never become the source of somebody else's replace.
    Exposed so tests (and a cleanup pass) can name it without duplicating the
    convention.
    """
    path = Path(path)
    return path.with_suffix(path.suffix + f".tmp.{os.getpid()}")


def write_text_atomic(path, text):
    """Write ``text`` to ``path`` via a temp file and an atomic replace.

    Returns the path written. Anything that goes wrong — a full disk, a
    permission error — removes the scratch file and re-raises, leaving the
    previous content of ``path`` untouched.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = temp_path_for(path)
    try:
        tmp.write_text(text)
        os.replace(tmp, path)
    except BaseException:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise
    return path


def write_json_atomic(path, data, *, indent=2):
    """Serialize ``data`` as JSON and write it atomically. Returns the path.

    Serializing before touching the filesystem means an unserializable object
    raises without having created a scratch file at all.
    """
    return write_text_atomic(path, json.dumps(data, indent=indent) + "\n")


def warn_unreadable(path, exc):
    """Report a state file that exists but could not be parsed.

    Every loader here answers an unparseable file with an empty result, because
    refusing to start the monitor over one bad file is worse than proceeding.
    That is a defensible policy but a terrible silence: with atomic writes in
    place, a parse failure no longer means "caught it mid-write", it means the
    file is genuinely damaged — and the next save will overwrite the evidence.
    Say so on stderr, where the daemon log will keep it.
    """
    print(f"[WARN] failed to parse {path}: {exc}", file=sys.stderr, flush=True)


__all__ = ["temp_path_for", "warn_unreadable", "write_json_atomic", "write_text_atomic"]
