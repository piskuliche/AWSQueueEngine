"""Client-side submit helpers: archive payload and upload to S3.

After Phase 2 the actual enqueue happens over the JSON-over-SSH RPC
(:mod:`awsqueueengine.shared.rpc_client`). After Phase 3 the S3 bucket
and prefix come from the resolved client config (CLI > env > config)
rather than being read directly from environment at import time.
"""
import tarfile
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

from .staging import sizeof_local_path_bytes

#: Default parallel uploads for a batch submit. Each worker holds a whole
#: tarball in the temp dir and saturates a share of the uplink, so this is
#: deliberately modest rather than derived from the CPU count — the work is
#: neither CPU-bound nor free in disk.
DEFAULT_UPLOAD_WORKERS = 4


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


def prepare_payload(payload_path, *, bucket, prefix):
    """Archive one payload, upload it, and drop the temp archive.

    The archive → upload → unlink sequence, extracted from ``cmd_submit_remote``
    so single and batch submit share one definition of it. Returns
    ``{"s3_uri", "size_bytes"}``; raises whatever the archive or upload raised,
    with the temp file cleaned up either way.
    """
    payload = Path(payload_path).expanduser()
    size_bytes = sizeof_local_path_bytes(payload)
    archive_path = None
    try:
        archive_path = archive_payload_to_temp(payload)
        s3_uri = upload_payload_archive_to_s3(
            archive_path, payload.name, bucket=bucket, prefix=prefix,
        )
    finally:
        if archive_path:
            try:
                archive_path.unlink()
            except OSError:
                pass
    return {"s3_uri": s3_uri, "size_bytes": size_bytes}


def upload_payloads_parallel(payload_paths, *, bucket, prefix, max_workers=None):
    """Prepare many payloads concurrently. Returns ``[result_or_exception, ...]``.

    Results stay **positional**, one entry per input in input order, so the
    caller can pair each with the job id it minted up front regardless of the
    order the uploads actually finished in.

    Threads rather than processes: the work is I/O-bound, and the two things
    that aren't (tar and gzip) release the GIL inside their C extensions.
    Thread-safety of the upload itself comes for free — ``boto3.client("s3")``
    is constructed per call, so no client is shared across workers.

    A failure is *returned*, not raised: at 105 payloads one bad directory must
    not abandon the other 104, and the caller has to report both sets.
    """
    paths = list(payload_paths)
    if not paths:
        return []
    workers = max(1, min(max_workers or DEFAULT_UPLOAD_WORKERS, len(paths)))

    def one(path):
        try:
            return prepare_payload(path, bucket=bucket, prefix=prefix)
        except Exception as exc:  # returned, not raised — see docstring
            return exc

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(one, paths))
