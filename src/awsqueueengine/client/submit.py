"""Client-side submit helpers: archive payload and upload to S3.

After Phase 2, the actual enqueue happens over the JSON-over-SSH RPC
(:mod:`awsqueueengine.shared.rpc_client`), so the SSH/CLI-proxy helpers
that used to live here are gone — see git history for the legacy form.
"""
import tarfile
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

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
