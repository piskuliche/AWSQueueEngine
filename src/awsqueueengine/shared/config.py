"""Shared configuration constants used by both client and host."""
import os
from pathlib import Path

HOSTS = [f"eci{i}" for i in range(1, 21)]
HOSTS_FILE = os.getenv("AWSQUEUEENGINE_HOSTS_FILE", "").strip()

SSH_BIN = "ssh"
RSYNC_BIN = "rsync"
SSH_TIMEOUT = 60

# Worker-side conventions: paths on worker hosts where the queue host stages
# payloads and writes per-job logs/pidfiles. Both client (tail, where) and
# host (submit_to_host, monitor) need to know these.
REMOTE_LOG_DIR = "/home/ubuntu/manager_jobs"
REMOTE_SCRATCH_ROOTS = ["/home/ubuntu/1scratch", "/home/ubuntu/2scratch"]

QUEUE_FILE = Path.home() / ".aws_slurm_like_queue.json"
RUNNING_FILE = Path.home() / ".aws_slurm_like_running.json"
COMPLETED_FILE = Path.home() / ".aws_slurm_like_completed.json"
DEFERRED_FILE = Path.home() / ".aws_slurm_like_deferred.json"
