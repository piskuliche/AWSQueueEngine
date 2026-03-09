# Configuration and constants for AWSQueueManager
import os
from pathlib import Path

HOSTS = [f"eci{i}" for i in range(1,21)]  # eci1..eci30
SSH_BIN = "ssh"
RSYNC_BIN = "rsync"
REMOTE_LOG_DIR = "/home/ubuntu/manager_jobs"
CHECK_INTERVAL = 60  # seconds between monitor checks
SSH_TIMEOUT = 60  # seconds for each ssh call
QUEUE_FILE = Path.home() / ".aws_slurm_like_queue.json"
RUNNING_FILE = Path.home() / ".aws_slurm_like_running.json"
COMPLETED_FILE = Path.home() / ".aws_slurm_like_completed.json"
MONITOR_STATE_FILE = Path.home() / ".aws_slurm_like_monitor_state.json"
REMOTE_SCRATCH_ROOTS = ["/home/ubuntu/1scratch", "/home/ubuntu/2scratch"]  # order = preference

# Mailtrap API alert configuration.
MAILTRAP_TOKEN = os.getenv("AWSQUEUEENGINE_MAILTRAP_TOKEN", "").strip()
MAILTRAP_SENDER_EMAIL = os.getenv("AWSQUEUEENGINE_MAILTRAP_SENDER_EMAIL", "").strip()
MAILTRAP_SENDER_NAME = os.getenv("AWSQUEUEENGINE_MAILTRAP_SENDER_NAME", "AWSQueueEngine").strip()
MAILTRAP_CATEGORY = os.getenv("AWSQUEUEENGINE_MAILTRAP_CATEGORY", "Queue Monitor").strip()
ALERT_DAILY_EMAIL_LIMIT = int(os.getenv("AWSQUEUEENGINE_ALERT_DAILY_EMAIL_LIMIT", "150"))
JOB_FAIL_ALERT_COOLDOWN_SECONDS = int(os.getenv("AWSQUEUEENGINE_JOB_FAIL_ALERT_COOLDOWN_SECONDS", "900"))
