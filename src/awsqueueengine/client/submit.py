"""Client-side submit helpers: archive payload, upload to S3, SSH-enqueue.

Phase 2 will route the SSH enqueue through the JSON-over-SSH protocol
(`shared/rpc_client.py`) instead of re-invoking the legacy CLI.
"""
import shlex
import subprocess
import tarfile
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

from ..shared.config import SSH_BIN
from .config import S3_BUCKET, S3_PREFIX


def archive_payload_to_temp(payload_path):
    payload = Path(payload_path).expanduser()
    if not payload.exists():
        raise FileNotFoundError(f"local payload not found: {payload}")
    tmp = tempfile.NamedTemporaryFile(prefix="awsqueueengine-payload-", suffix=".tar.gz", delete=False)
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        with tarfile.open(tmp_path, "w:gz") as tar:
            if payload.is_dir():
                children = list(payload.iterdir())
                if children:
                    for child in children:
                        tar.add(child, arcname=child.name)
                else:
                    tar.add(payload, arcname=".")
            else:
                tar.add(payload, arcname=payload.name)
    except Exception:
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise
    return tmp_path


def upload_payload_archive_to_s3(archive_path, payload_name):
    if not S3_BUCKET:
        raise RuntimeError("AWSQUEUEENGINE_S3_BUCKET is required for remote submit with --payload.")
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for remote submit with --payload.") from exc

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    clean_name = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in payload_name) or "payload"
    key_parts = [part for part in (S3_PREFIX, f"{timestamp}-{uuid.uuid4().hex}", f"{clean_name}.tar.gz") if part]
    key = "/".join(key_parts)
    boto3.client("s3").upload_file(str(archive_path), S3_BUCKET, key)
    return f"s3://{S3_BUCKET}/{key}"


def build_remote_submit_argv(args, command, payload_s3_uri=None, payload_size_bytes=None, job_id=None):
    argv = ["awsqueueengine", "submit"]
    queue_name = getattr(args, "queue", None) or getattr(args, "host_set", None)
    if queue_name:
        argv.extend(["--queue", queue_name])
    if getattr(args, "hosts", None):
        for host_value in args.hosts:
            argv.extend(["--hosts", host_value])
    if args.priority is not None:
        argv.extend(["--priority", str(args.priority)])
    elif args.high_priority:
        argv.append("--high-priority")
    if getattr(args, "preempt", False):
        argv.append("--preempt")
    if payload_s3_uri:
        argv.extend(["--payload-s3-uri", payload_s3_uri])
    if payload_size_bytes is not None:
        argv.extend(["--payload-size-bytes", str(payload_size_bytes)])
    if job_id:
        argv.extend(["--job-id", job_id])
    argv.append(command)
    return argv


def run_remote_submit(queue_host, remote_argv):
    remote_cmd = shlex.join(remote_argv)
    return subprocess.run([SSH_BIN, queue_host, remote_cmd], capture_output=True, text=True, check=False)
