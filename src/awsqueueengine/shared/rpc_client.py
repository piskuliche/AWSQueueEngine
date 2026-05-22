"""Client-side helper for invoking the host RPC over SSH.

::

    from awsqueueengine.shared.rpc_client import call
    result = call("queue-manager", "list", {})

Transport failures (SSH non-zero, malformed response, non-JSON output) raise
:class:`RpcTransportError`. Application-level failures (unknown method,
validation, etc.) raise :class:`RpcError` with a structured code/message.
"""
from __future__ import annotations

import json
import subprocess
from typing import Any, Callable

from .config import SSH_BIN
from .protocol import (
    PROTOCOL_VERSION,
    RpcError,
    RpcTransportError,
    make_request,
)

DEFAULT_TIMEOUT = 60
HOST_RPC_ARGV = ("awsqe-host", "rpc")


def call(
    host: str,
    method: str,
    params: dict | None = None,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    ssh_bin: str = SSH_BIN,
    transport: Callable[[list[str], str, int], subprocess.CompletedProcess] | None = None,
) -> Any:
    """Send one RPC to `host`. Returns the ``result`` field on success.

    ``transport`` is an injection seam for tests: a callable that takes
    ``(argv, stdin_text, timeout)`` and returns an object with
    ``returncode``, ``stdout``, ``stderr`` attributes.
    """
    request = make_request(method, params)
    raw = json.dumps(request)
    argv = [ssh_bin, host, *HOST_RPC_ARGV]
    try:
        if transport is None:
            proc = subprocess.run(
                argv,
                input=raw,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        else:
            proc = transport(argv, raw, timeout)
    except subprocess.TimeoutExpired as exc:
        raise RpcTransportError(124, f"ssh timeout after {timeout}s") from exc

    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip() or "(no output)"
        raise RpcTransportError(proc.returncode, detail)

    stdout = proc.stdout or ""
    try:
        response = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RpcTransportError(0, f"non-JSON response: {exc}; raw={stdout!r}") from exc

    if not isinstance(response, dict) or response.get("version") != PROTOCOL_VERSION:
        raise RpcTransportError(0, f"bad response envelope: {response!r}")

    if response.get("ok"):
        result = response.get("result")
        return result if result is not None else {}

    err = response.get("error") or {}
    raise RpcError(
        code=str(err.get("code") or "unknown"),
        message=str(err.get("message") or "(no message)"),
    )
