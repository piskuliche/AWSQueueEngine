"""Client-side helpers that talk to the queue host over SSH.

Phase 2 will replace `proxy_remote_cli` / `query_job_state_remote` with
calls into `shared.rpc_client` against the versioned JSON protocol.
"""
import json
import shlex
import subprocess
import sys

from ..shared.config import SSH_BIN


def proxy_remote_cli(queue_host, remote_argv):
    """Run an awsqueueengine CLI command on a remote queue host and stream its output."""
    remote_cmd = shlex.join(remote_argv)
    result = subprocess.run([SSH_BIN, queue_host, remote_cmd], capture_output=True, text=True, check=False)
    if result.stdout:
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n", flush=True)
    if result.stderr:
        print(result.stderr, end="" if result.stderr.endswith("\n") else "\n", file=sys.stderr, flush=True)
    if result.returncode != 0:
        sys.exit(result.returncode)


def query_job_state_remote(queue_host, job_id):
    remote_argv = ["awsqueueengine", "job-info", job_id]
    remote_cmd = shlex.join(remote_argv)
    result = subprocess.run([SSH_BIN, queue_host, remote_cmd], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"remote job-info failed (rc={result.returncode}): {detail}")
    raw = (result.stdout or "").strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"could not parse remote job-info output: {exc}; raw={raw!r}")
    if not parsed:
        return None
    return parsed
