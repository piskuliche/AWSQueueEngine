# Job submission and control
import uuid
import shlex
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from .config import REMOTE_LOG_DIR, SSH_TIMEOUT, HOSTS
from .ssh_utils import ssh_run
from .staging import sizeof_local_path_bytes, choose_scratch_on_host, rsync_to_host_with_fallback
from .queue import load_queue, save_queue, dequeue


def _payload_name_from_s3_uri(payload_s3_uri):
    parsed = urlparse(payload_s3_uri)
    name = Path(parsed.path).name
    if name.endswith(".tar.gz"):
        name = name[:-7]
    return name or "payload"


def new_job_tag():
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"{timestamp}-{uuid.uuid4().hex[:6]}"


_new_job_tag = new_job_tag  # backward-compat alias


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

def kill_managed_on_host(host, ssh_run=ssh_run, grace_seconds=3):
    cmd = r"""
pidfiles=$(ls -1 {remote_log_dir}/*.pid 2>/dev/null || true)
roots=""
tracked_pidfiles=""
for pidfile in $pidfiles; do
  pid=$(cat "$pidfile" 2>/dev/null | tr -d '[:space:]')
  if [ -z "$pid" ]; then
    continue
  fi
  if ps -p "$pid" -o pid= >/dev/null 2>&1; then
    roots="$roots $pid"
    tracked_pidfiles="$tracked_pidfiles $pidfile"
  fi
done
if [ -z "$roots" ]; then
  roots=$(pgrep -f '[M]ANAGER_TAG=' || true)
fi
if [ -n "$roots" ]; then
  all=""
  for root in $roots; do
    queue="$root"
    descendants="$root"
    while [ -n "$queue" ]; do
      next=""
      for q in $queue; do
        kids=$(pgrep -P "$q" 2>/dev/null || true)
        if [ -n "$kids" ]; then
          next="$next $kids"
        fi
      done
      queue=$(echo $next)
      if [ -n "$queue" ]; then
        descendants="$descendants $queue"
      fi
    done
    all="$all $descendants"
  done
  final=$(echo $all | tr ' ' '\n' | grep -E '.' | sort -n | uniq | tr '\n' ' ')
  if [ -n "$final" ]; then
    kill -TERM $final 2>/dev/null || true
    sleep {grace}
    kill -KILL $final 2>/dev/null || true
  fi
fi
for pidfile in $tracked_pidfiles; do
  rm -f "$pidfile" 2>/dev/null || true
done
pkill -f '[p]memd.cuda' || true
pkill -f '[p]memd.cuda.MPI' || true
exit 0
""".format(grace=int(grace_seconds), remote_log_dir=REMOTE_LOG_DIR)
    rc, out, err = ssh_run(host, cmd)
    return {"host": host, "rc": rc, "out": out, "err": err}

def tail_remote_log(host, lines=200):
    from .host_status import check_host_for_tag
    check = check_host_for_tag(host)
    if not check["reachable"]:
        return {"host": host, "ok": False, "reason": "unreachable"}
    tag = check.get("tag")
    if tag:
        path = f"{REMOTE_LOG_DIR}/{tag}.log"
    else:
        rc, out, err = ssh_run(host, f"ls -t {REMOTE_LOG_DIR}/*.log 2>/dev/null | head -n1 || true")
        if rc != 0 or not out:
            return {"host": host, "ok": True, "tag": None, "out": "(no log found)"}
        path = out.strip()
    rc, out, err = ssh_run(host, f"tail -n {lines} {path} || true")
    return {"host": host, "ok": True, "tag": tag, "out": out, "err": err}

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
