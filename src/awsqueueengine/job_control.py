# Job submission and control
import uuid
import shlex
import time
from pathlib import Path
from .config import REMOTE_LOG_DIR, SSH_TIMEOUT, HOSTS
from .ssh_utils import ssh_run
from .staging import sizeof_local_path_bytes, choose_scratch_on_host, rsync_to_host_with_fallback
from .queue import load_queue, save_queue, dequeue

def submit_to_host(host, job_command, payload_local_path=None, payload_remote_path=None):
    tag = uuid.uuid4().hex[:12]
    remote_payload_dir = None
    if payload_remote_path:
        remote_payload_dir = str(payload_remote_path).strip() or None
    if not payload_local_path and not remote_payload_dir:
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
            return {"host": host, "tag": tag, "pid": None, "ok": False, "err": err or out}
    if not remote_payload_dir:
        local_path = Path(payload_local_path).expanduser()
        if not local_path.exists():
            return {"host": host, "ok": False, "err": f"local payload not found: {local_path}"}
        needed_bytes = sizeof_local_path_bytes(local_path)
        remote_root, info = choose_scratch_on_host(host, needed_bytes)
        if not remote_root:
            return {"host": host, "ok": False, "err": f"no suitable scratch: {info}"}
        jobname = Path(payload_local_path).name
        remote_payload_dir = f"{remote_root}/{jobname}-{tag}"
        rc, out, err = ssh_run(host, f"mkdir -p {remote_root} && chmod 700 {remote_root}", timeout=30)
        if rc != 0:
            return {"host": host, "ok": False, "err": f"mkdir failed: {err or out}"}
        ok, method, sout, serr = rsync_to_host_with_fallback(str(local_path), host, remote_payload_dir)
        if not ok:
            return {"host": host, "ok": False, "err": f"rsync failed: {serr or sout}"}
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
            return {"host": host, "tag": tag, "pid": pid, "ok": False, "err": "pidfile present but process not running", "payload": remote_payload_dir}
    rc4, out4, _ = ssh_run(host, "ps -eo pid,cmd | grep -F 'MANAGER_TAG=' | grep -v grep || true", timeout=8)
    if out4:
        try:
            pid_guess = out4.splitlines()[0].split(None, 1)[0]
            return {"host": host, "tag": tag, "pid": pid_guess, "ok": True, "payload": remote_payload_dir, "note": "started-no-pidfile"}
        except Exception:
            pass
    return {"host": host, "tag": tag, "pid": None, "ok": False, "err": err or out, "payload": remote_payload_dir}

def kill_managed_on_host(host, ssh_run=ssh_run, grace_seconds=3):
    cmd = r"""
pids=$(pgrep -f 'MANAGER_TAG=' || true)
if [ -n "$pids" ]; then
  all=""
  for root in $pids; do
    queue="$root"
    descendants="$root"
    while [ -n "$queue" ]; do
      next=""
      for q in $queue; do
        kids=$(pgrep -P "$q" -d ' ' 2>/dev/null || true)
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
pkill -f 'pmemd.cuda' || true
pkill -f 'pmemd.cuda.MPI' || true
""".format(grace=int(grace_seconds))
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
