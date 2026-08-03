"""JSON-over-SSH protocol between client and host.

Wire format (one request, one response, per SSH invocation):

    request:  {"version": 1, "method": "<name>", "params": {...}}
    response: {"version": 1, "ok": true,  "result": {...}}
    response: {"version": 1, "ok": false, "error": {"code": "...", "message": "..."}}

Transport: the client invokes ``ssh <queue_host> awsqe-host rpc`` and pipes
the request JSON on stdin. The host writes a single response JSON object to
stdout and exits 0.

A non-zero exit code from ``awsqe-host rpc`` always means a transport- or
daemon-level failure (SSH, malformed input, uncaught exception that never
got serialized). Application-level errors (unknown method, validation
failure, job not found, …) are returned in the response body with
``ok: false`` and a structured ``error`` field, and the exit code is still 0.

Version policy
--------------
- ``version`` is the WIRE format version. Bump it only on incompatible
  changes to the envelope (e.g., switching to a different framing).
- Per-method param/result schemas can evolve additively within a version;
  clients should ignore unknown response fields.

Error codes
-----------
- ``bad_request``     — malformed JSON or wrong envelope shape
- ``unknown_method``  — method not registered on the host
- ``invalid_params``  — params failed validation for the method
- ``not_found``       — entity referenced by the request does not exist
- ``conflict``        — request is well-formed but rejected by current state
- ``internal``        — uncaught server-side error (bug)
"""
from dataclasses import dataclass

PROTOCOL_VERSION = 1


@dataclass
class RpcError(Exception):
    """Application-level error returned in the response envelope."""
    code: str
    message: str

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


@dataclass
class RpcTransportError(Exception):
    """Transport- or daemon-level failure (SSH, JSON parse, non-zero exit)."""
    returncode: int
    detail: str

    def __str__(self) -> str:
        return f"rpc transport error (rc={self.returncode}): {self.detail}"


def make_request(method: str, params: dict | None = None) -> dict:
    """Build a request envelope for ``method``.

    Args:
        method: Registered method name on the host.
        params: Method params; ``None`` becomes ``{}`` so the host always sees an
            object rather than a missing key.

    Returns:
        The envelope to write to ``awsqe-host rpc`` on stdin.
    """
    return {"version": PROTOCOL_VERSION, "method": method, "params": params or {}}


def make_ok(result: dict | None = None) -> dict:
    """Build a success response envelope.

    Args:
        result: Method result; ``None`` becomes ``{}``.

    Returns:
        An envelope with ``ok: true``.
    """
    return {"version": PROTOCOL_VERSION, "ok": True, "result": result or {}}


def make_error(code: str, message: str) -> dict:
    """Build a failure response envelope.

    This is an *application-level* failure, which still exits 0 — see the
    exit-code contract in the module docstring above.

    Args:
        code: One of the error codes listed in the module docstring.
        message: Human-readable detail, surfaced to the user by the client.

    Returns:
        An envelope with ``ok: false``.
    """
    return {
        "version": PROTOCOL_VERSION,
        "ok": False,
        "error": {"code": code, "message": message},
    }
