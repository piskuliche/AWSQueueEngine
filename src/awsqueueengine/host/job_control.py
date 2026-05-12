"""Host-side job placement on worker hosts.

`submit_to_host` is the workhorse called by the monitor. `write_run_info`
writes the run.info file in the submitter's payload directory.
"""
import shlex
from pathlib import Path
from urllib.parse import urlparse

from ..shared.config import REMOTE_LOG_DIR, SSH_TIMEOUT
from ..shared.ssh_utils import ssh_run
from ..shared.worker_actions import new_job_tag
from ..shared.worker_staging import (
    choose_scratch_on_host,
    rsync_to_host_with_fallback,
    sizeof_local_path_bytes,
)


def _payload_name_from_s3_uri(payload_s3_uri):
    parsed = urlparse(payload_s3_uri)
    name = Path(parsed.path).name
    if name.endswith(".tar.gz"):
        name = name[:-7]
    return name or "payload"


def submit_to_host(
    host,
    job_command,
    payload_local_path=None,
    payload_remote_path=None,
    payload_s3_uri=None,
    payload_size_bytes=None,
    tag=None,
):
    tag = tag or new_job_tag()
    remote_payload_dir = None
    if payload_remote_path:
        remote_payload_dir = str(payload_remote_path).strip() or None
    if not payload_local_path and not remote_payload_dir and not payload_s3_uri:
        remote_cmd = (
            rf"mkdir -p {REMOTE_LOG_DIR} && cd $HOME || true && "
            rf"nohup env MANAGER_TAG={tag} bash -lc {shlex.quote(job_command)} > {REMOTE_LOG_DIR}/{tag}.log 2>&1 < /dev/null & echo $! > {REMOTE_LOG_DIR}/{tag}.pid"
        )
        rc, out, err = ssh_run(host, remote_cmd, timeout=SSH_TIMEOUT)
        if rc == 0:
            rc2, out2, err2 = ssh_run(host, f"cat {REMOTE_LOG_DIR}/{tag}.pid || true")
            pid = out2.strip() if out2 else None
            return {"host": host, "tag": tag, "pid": pid, "ok": True}
        else:
            return {"host": host, "tag": tag, "pid": None, "ok": False, "err": err or out, "reason": "host_transport"}
    if payload_s3_uri and not remote_payload_dir:
        try:
            needed_bytes = int(payload_size_bytes or 0)
        except (TypeError, ValueError):
            needed_bytes = 0
        remote_root, info = choose_scratch_on_host(host, needed_bytes)
        if not remote_root:
            return {"host": host, "ok": False, "err": f"no suitable scratch: {info}", "reason": "host_storage"}
        jobname = _payload_name_from_s3_uri(payload_s3_uri)
        remote_payload_dir = f"{remote_root}/{jobname}-{tag}"
        archive_path = f"{remote_payload_dir}/payload.tar.gz"
        download_cmd = "\n".join(
            [
                "set -euo pipefail",
                f"mkdir -p {shlex.quote(remote_payload_dir)}",
                f"chmod 700 {shlex.quote(remote_payload_dir)}",
                f"aws s3 cp {shlex.quote(payload_s3_uri)} {shlex.quote(archive_path)}",
                f"tar xzf {shlex.quote(archive_path)} -C {shlex.quote(remote_payload_dir)}",
                f"rm -f {shlex.quote(archive_path)}",
            ]
        )
        rc, out, err = ssh_run(host, f"bash -lc {shlex.quote(download_cmd)}", timeout=900)
        if rc != 0:
            return {
                "host": host,
                "tag": tag,
                "ok": False,
                "err": f"s3 payload download failed: {err or out}",
                "payload": remote_payload_dir,
                "reason": "host_transport",
            }
    if not remote_payload_dir:
        local_path = Path(payload_local_path).expanduser()
        if not local_path.exists():
            return {"host": host, "ok": False, "err": f"local payload not found: {local_path}", "reason": "job"}
        needed_bytes = sizeof_local_path_bytes(local_path)
        remote_root, info = choose_scratch_on_host(host, needed_bytes)
        if not remote_root:
            return {"host": host, "ok": False, "err": f"no suitable scratch: {info}", "reason": "host_storage"}
        jobname = Path(payload_local_path).name
        remote_payload_dir = f"{remote_root}/{jobname}-{tag}"
        rc, out, err = ssh_run(host, f"mkdir -p {remote_root} && chmod 700 {remote_root}", timeout=30)
        if rc != 0:
            return {"host": host, "ok": False, "err": f"mkdir failed: {err or out}", "payload": remote_payload_dir, "reason": "host_transport"}
        ok, method, sout, serr = rsync_to_host_with_fallback(str(local_path), host, remote_payload_dir)
        if not ok:
            return {"host": host, "ok": False, "err": f"rsync failed: {serr or sout}", "reason": "host_transport"}
    remote_cmd = (
        rf"mkdir -p {REMOTE_LOG_DIR} && cd $HOME || true && "
        rf"nohup env MANAGER_TAG={tag} PAYLOAD_DIR={remote_payload_dir} bash -lc {shlex.quote(job_command)} "
        rf"> {REMOTE_LOG_DIR}/{tag}.log 2>&1 < /dev/null & echo $! > {REMOTE_LOG_DIR}/{tag}.pid"
    )
    rc, out, err = ssh_run(host, remote_cmd, timeout=SSH_TIMEOUT)
    rc2, pid_out, err2 = ssh_run(host, f"cat {REMOTE_LOG_DIR}/{tag}.pid || true", timeout=5)
    pid = pid_out.strip() if pid_out else None
    if pid:
        rc3, ps_out, err3 = ssh_run(host, f"ps -p {pid} -o pid= || true", timeout=5)
        if ps_out.strip():
            return {"host": host, "tag": tag, "pid": pid, "ok": True, "payload": remote_payload_dir}
        else:
            return {"host": host, "tag": tag, "pid": pid, "ok": False, "err": "pidfile present but process not running", "payload": remote_payload_dir, "reason": "job"}
    rc4, out4, _ = ssh_run(host, "ps -eo pid,cmd | grep -F 'MANAGER_TAG=' | grep -v grep || true", timeout=8)
    if out4:
        try:
            pid_guess = out4.splitlines()[0].split(None, 1)[0]
            return {"host": host, "tag": tag, "pid": pid_guess, "ok": True, "payload": remote_payload_dir, "note": "started-no-pidfile"}
        except Exception:
            pass
    return {"host": host, "tag": tag, "pid": None, "ok": False, "err": err or out, "payload": remote_payload_dir, "reason": "job"}


def write_run_info(local_payload_path, jobid, host, remote_payload_path):
    if not local_payload_path:
        return
    p = Path(local_payload_path).expanduser()
    try:
        p.mkdir(parents=True, exist_ok=True)
        info_path = p / "run.info"
        info_path.write_text(f"{jobid}\n{host}\n{remote_payload_path}\n")
    except Exception as e:
        print(f"[WARN] failed to write run.info in {p}: {e}")
