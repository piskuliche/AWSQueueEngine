# Configuration and constants for AWSQueueManager
from pathlib import Path

HOSTS = [f"eci{i}" for i in range(1,21)]  # eci1..eci30
SSH_BIN = "ssh"
RSYNC_BIN = "rsync"
REMOTE_LOG_DIR = "/home/ubuntu/manager_jobs"
CHECK_INTERVAL = 60  # seconds between monitor checks
SSH_TIMEOUT = 60  # seconds for each ssh call
QUEUE_FILE = Path.home() / ".aws_slurm_like_queue.json"
RUNNING_FILE = Path.home() / ".aws_slurm_like_running.json"
REMOTE_SCRATCH_ROOTS = ["/home/ubuntu/1scratch", "/home/ubuntu/2scratch"]  # order = preference
