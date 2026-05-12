"""Worker-side staging helpers: scratch selection and rsync transport.

Used by client (`where` probes free scratch on each host) and host
(`submit_to_host` selects scratch and rsyncs the payload).
"""
import shlex
import subprocess
from pathlib import Path
from typing import Optional, Tuple

from .config import REMOTE_SCRATCH_ROOTS, RSYNC_BIN, SSH_BIN
from .ssh_utils import ssh_run

MAX_USED_BYTES = int(1.5 * 1024 ** 4)  # 1.5 TiB; refuse mounts past this


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
    """Pick a scratch mount on `host` with >= `needed_bytes` free.

    Prefers the mount with the smallest used bytes. Refuses mounts past
    MAX_USED_BYTES used regardless of free space.

    Returns (mountpoint, available_bytes) on success or (None, error_string).
    """
    cmd = "df -P -B1 " + " ".join(shlex.quote(p) for p in REMOTE_SCRATCH_ROOTS) + " 2>/dev/null || true"
    rc, out, err = ssh_run(host, cmd)
    if rc != 0 and not out:
        return (None, f"df failed: {err or '(no output)'}")

    lines = [l for l in out.splitlines() if l.strip()]
    if not lines:
        return (None, "df produced no output")

    parsed = []
    for line in lines:
        parts = line.split()
        if len(parts) < 6:
            continue
        try:
            mountpoint = parts[-1]
            avail = int(parts[-3])
            used = int(parts[-4])
            size = int(parts[-5])
            usepct_str = parts[-2]
            usepct = int(usepct_str.rstrip('%')) if usepct_str.endswith('%') else None
        except Exception:
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

    acceptable_by_usage = [p for p in parsed if p["used"] <= MAX_USED_BYTES]
    candidates = [p for p in acceptable_by_usage if p["avail"] >= needed_bytes]

    if candidates:
        best = min(candidates, key=lambda p: p["used"])
        return (best["mountpoint"], best["avail"])

    big_used_but_enough = [p for p in parsed if p["avail"] >= needed_bytes and p["used"] > MAX_USED_BYTES]
    if big_used_but_enough:
        mp = big_used_but_enough[0]
        return (None, (f"refusing to use {mp['mountpoint']}: used {mp['used']} bytes > "
                       f"{MAX_USED_BYTES} (max allowed); but it has {mp['avail']} bytes available"))

    best_avail = max(parsed, key=lambda p: p["avail"])
    return (None, f"insufficient space: best={best_avail['mountpoint']} has {best_avail['avail']} bytes free; need {needed_bytes}")
