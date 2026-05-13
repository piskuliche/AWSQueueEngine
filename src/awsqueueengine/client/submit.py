"""Client-side submit helpers: archive payload and upload to S3.

After Phase 2 the actual enqueue happens over the JSON-over-SSH RPC
(:mod:`awsqueueengine.shared.rpc_client`). After Phase 3 the S3 bucket
and prefix come from the resolved client config (CLI > env > config)
rather than being read directly from environment at import time.
"""
import tarfile
import tempfile
import uuid
from datetime import datetime
from pathlib import Path


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


def upload_payload_archive_to_s3(archive_path, payload_name, *, bucket, prefix):
    if not bucket:
        raise RuntimeError(
            "S3 bucket is required for remote submit with --payload. "
            "Set AWSQUEUEENGINE_S3_BUCKET or run `awsqe-client config set s3.bucket <name>`."
        )
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for remote submit with --payload.") from exc

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    clean_name = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in payload_name) or "payload"
    clean_prefix = (prefix or "").strip().strip("/")
    key_parts = [part for part in (clean_prefix, f"{timestamp}-{uuid.uuid4().hex}", f"{clean_name}.tar.gz") if part]
    key = "/".join(key_parts)
    boto3.client("s3").upload_file(str(archive_path), bucket, key)
    return f"s3://{bucket}/{key}"
