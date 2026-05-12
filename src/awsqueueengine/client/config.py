"""Client-only configuration: S3 payload bucket/prefix.

Phase 3 will extend this with a ClientConfig dataclass backed by
~/.awsqe/client/config.toml so the user can persist `queue_host`,
`s3_bucket`, etc., and avoid passing flags every invocation.
"""
import os

S3_BUCKET = os.getenv("AWSQUEUEENGINE_S3_BUCKET", "").strip()
S3_PREFIX = os.getenv("AWSQUEUEENGINE_S3_PREFIX", "awsqueueengine/payloads").strip().strip("/")
