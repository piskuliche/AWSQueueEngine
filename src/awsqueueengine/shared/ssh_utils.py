# SSH utility functions
import subprocess
from .config import SSH_BIN, SSH_TIMEOUT

def ssh_run(host, cmd, timeout=SSH_TIMEOUT, capture_output=True):
    ssh_cmd = [SSH_BIN, host, cmd]
    try:
        result = subprocess.run(
            ssh_cmd,
            capture_output=capture_output,
            text=True,
            timeout=timeout,
            check=False
        )
        return result.returncode, result.stdout.strip(), result.stderr.strip()
    except subprocess.TimeoutExpired:
        return 124, "", "ssh timeout"
