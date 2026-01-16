# Staging/transfer utilities
import subprocess
from pathlib import Path
import shlex
from .config import RSYNC_BIN, SSH_BIN, REMOTE_SCRATCH_ROOTS, HOSTS
from .ssh_utils import ssh_run

import shlex
from typing import Optional, Tuple

# keep your existing REMOTE_SCRATCH_ROOTS, ssh_run available in scope
# Example:
# REMOTE_SCRATCH_ROOTS = ["/scratch1", "/scratch2", "/lscratch"]
# ssh_run(host, cmd) -> (rc, stdout, stderr)

MAX_USED_BYTES = int(1.5 * 1024 ** 4)  # 1.5 TiB in bytes (change if you want decimal TB)

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




def choose_scratch_on_host(host: str, needed_bytes: int) -> Tuple[Optional[str], int | str]:
    """
    Choose a scratch mount on `host` that has at least `needed_bytes` free,
    preferring the mount with the least data currently stored (smallest used bytes).
    Do not select mounts that already have > MAX_USED_BYTES used.

    Returns:
        (mountpoint, available_bytes) on success
        (None, error_string) on failure
    """
    # Use df with block size 1 to get raw bytes (POSIX -P + -B1)
    cmd = "df -P -B1 " + " ".join(shlex.quote(p) for p in REMOTE_SCRATCH_ROOTS) + " 2>/dev/null || true"
    rc, out, err = ssh_run(host, cmd)
    if rc != 0 and not out:
        return (None, f"df failed: {err or '(no output)'}")

    lines = [l for l in out.splitlines() if l.strip()]
    if not lines:
        return (None, "df produced no output")

    # Skip header if present and parse the rest
    parsed = []
    for line in lines:
        parts = line.split()
        # A valid df -P line should have at least 6 columns:
        # Filesystem Size Used Avail Use% Mounted_on
        if len(parts) < 6:
            continue
        try:
            mountpoint = parts[-1]
            avail = int(parts[-3])   # Avail in bytes because of -B1
            used = int(parts[-4])    # Used in bytes
            size = int(parts[-5])    # Size in bytes
            usepct_str = parts[-2]   # like "12%"
            usepct = int(usepct_str.rstrip('%')) if usepct_str.endswith('%') else None
        except Exception:
            # If parsing any numeric field fails, skip this line
            continue

        if mountpoint in REMOTE_SCRATCH_ROOTS:
            parsed.append({
                "mountpoint": mountpoint,
                "size": size,
                "used": used,
                "avail": avail,
                "usepct": usepct,
            })

    if not parsed:
        return (None, "no scratch mountpoints found in df output")

    # Filter out mounts that exceed MAX_USED_BYTES (we don't want to submit there)
    acceptable_by_usage = [p for p in parsed if p["used"] <= MAX_USED_BYTES]
    # Among acceptable mounts, find those that have enough available bytes
    candidates = [p for p in acceptable_by_usage if p["avail"] >= needed_bytes]

    if candidates:
        # choose the candidate with the smallest used bytes (least filled)
        best = min(candidates, key=lambda p: p["used"])
        return (best["mountpoint"], best["avail"])

    # No candidate among mounts under the MAX_USED_BYTES limit.
    # Provide helpful diagnostics:
    # 1) Are there mounts that would satisfy size but exceed MAX_USED_BYTES?
    big_used_but_enough = [p for p in parsed if p["avail"] >= needed_bytes and p["used"] > MAX_USED_BYTES]
    if big_used_but_enough:
        mp = big_used_but_enough[0]
        return (None, (f"refusing to use {mp['mountpoint']}: used {mp['used']} bytes > "
                       f"{MAX_USED_BYTES} (max allowed); but it has {mp['avail']} bytes available"))

    # 2) If none have enough space, return the mount with the most available (as diagnostic)
    best_avail = max(parsed, key=lambda p: p["avail"])
    return (None, f"insufficient space: best={best_avail['mountpoint']} has {best_avail['avail']} bytes free; need {needed_bytes}")

def where_is_next_submit():
    for host in HOSTS:

        # small, non-invasive request (1 GiB)
        needed_bytes = 1 * 1024 ** 3

        mount, result = choose_scratch_on_host(host, needed_bytes)

        if mount is not None:
            free_bytes = result
            print(f"[OK] host={host} mount={mount} free={free_bytes} bytes")
        else:
            error = result
            print(f"[FAIL] host={host} error={error}")
