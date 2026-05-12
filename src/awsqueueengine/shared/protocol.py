"""JSON-over-SSH protocol between client and host.

Phase 2 fills this in. The wire format is::

    request:  {"version": 1, "method": "<name>", "params": {...}}
    response: {"version": 1, "ok": true,  "result": {...}}
    response: {"version": 1, "ok": false, "error": {"code": "...", "message": "..."}}

Host entry point: ``awsqe-host rpc`` reads one request from stdin and
writes one response to stdout.
"""

PROTOCOL_VERSION = 1
