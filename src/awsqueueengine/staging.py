# Staging/transfer utilities
import subprocess
from pathlib import Path
import shlex
from .config import RSYNC_BIN, SSH_BIN, REMOTE_SCRATCH_ROOTS
from .ssh_utils import ssh_run

def sizeof_local_path_bytes(local_path):
    p = Path(local_path).expanduser()
    if not p.exists():
        return 0
    if p.is_file():
        return p.stat().st_size
    total = 0
    for f in p.rglob('*'):
        if f.is_file():
            try:
                total += f.stat().st_size
            except Exception:
                pass
    return total

def rsync_to_host(local_path, host, remote_target_dir, timeout=900):
    local = str(Path(local_path).expanduser())
    remote_target = f"{host}:{remote_target_dir}/"
    cmd = [RSYNC_BIN, "-az", "--delete", "-e", SSH_BIN, local, remote_target]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=timeout)
        return (r.returncode == 0, r.stdout, r.stderr)
    except subprocess.TimeoutExpired:
        return (False, "", "rsync timeout")

def rsync_to_host_with_fallback(local_path, host, remote_target_dir, timeout=900):
    local = Path(local_path).expanduser()
    if not local.exists():
        return False, "none", "", f"local path not found: {local}"
    if local.is_dir():
        rsync_source = str(local.as_posix().rstrip("/")) + "/"
    else:
        rsync_source = str(local.as_posix())
    remote_target = f"{host}:{remote_target_dir}/"
    cmd = [RSYNC_BIN, "-az", "--delete", "-e", SSH_BIN, rsync_source, remote_target]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=timeout)
    except subprocess.TimeoutExpired:
        return False, "rsync", "", "rsync timeout"
    if r.returncode == 0:
        return True, "rsync", r.stdout, r.stderr
    serr = (r.stderr or "").lower()
    if "command not found" in serr or "rsync" in serr:
        local_parent = str(local) if local.is_dir() else str(local.parent)
        tar_cmd = ["tar", "czf", "-", "-C", str(local), "."] if local.is_dir() else ["tar", "czf", "-", "-C", str(local.parent), str(local.name)]
        ssh_ex_cmd = f"mkdir -p {shlex.quote(remote_target_dir)} && tar xzf - -C {shlex.quote(remote_target_dir)}"
        try:
            p_tar = subprocess.Popen(tar_cmd, stdout=subprocess.PIPE)
            p_ssh = subprocess.Popen([SSH_BIN, host, ssh_ex_cmd], stdin=p_tar.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            p_tar.stdout.close()
            out, err = p_ssh.communicate(timeout=timeout)
            rc = p_ssh.returncode
            if rc == 0:
                return True, "tar+ssh", out, err
            else:
                return False, "tar+ssh", out, err
        except subprocess.TimeoutExpired:
            return False, "tar+ssh", "", "tar+ssh timeout"
    return False, "rsync", r.stdout, r.stderr

def choose_scratch_on_host(host, needed_bytes):
    cmd = "df -Pk " + " ".join(shlex.quote(p) for p in REMOTE_SCRATCH_ROOTS) + " 2>/dev/null || true"
    rc, out, err = ssh_run(host, cmd)
    if rc != 0 and not out:
        return (None, f"df failed: {err}")
    lines = [l for l in out.splitlines() if l.strip()]
    best = None
    for line in lines:
        parts = line.split()
        if len(parts) < 6:
            continue
        mountpoint = parts[-1]
        try:
            available_kb = int(parts[3])
        except Exception:
            continue
        available_bytes = available_kb * 1024
        if mountpoint in REMOTE_SCRATCH_ROOTS:
            if available_bytes >= needed_bytes:
                return (mountpoint, available_bytes)
            if best is None or available_bytes > best[1]:
                best = (mountpoint, available_bytes)
    if best:
        return (None, f"insufficient space: best={best[0]} has {best[1]} bytes free; need {needed_bytes}")
    return (None, "no scratch mountpoints found")
