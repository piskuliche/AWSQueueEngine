"""SSH actions against worker hosts that both client and host invoke.

- `new_job_tag()`: stable job-tag generator. Client pre-generates job IDs for
  remote submit; host generates tags when launching jobs.
- `kill_managed_on_host()`: host calls during preempt/requeue; client calls
  for `stop <host>` and `requeue-running`.
- `tail_remote_log()`: client calls for `tail <host>`.
"""
import shlex
import uuid
from datetime import datetime

from .config import REMOTE_LOG_DIR
from .ssh_utils import ssh_run


def new_job_tag():
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"{timestamp}-{uuid.uuid4().hex[:6]}"


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
    # Tags come from the worker's `ps` output (MANAGER_TAG=...) and the
    # log filenames it produced. Treat as untrusted input on the shell.
    try:
        lines_n = int(lines)
    except (TypeError, ValueError):
        lines_n = 200
    log_dir_q = shlex.quote(REMOTE_LOG_DIR)
    tag = check.get("tag")
    if tag:
        path = f"{REMOTE_LOG_DIR}/{tag}.log"
    else:
        rc, out, err = ssh_run(host, f"ls -t {log_dir_q}/*.log 2>/dev/null | head -n1 || true")
        if rc != 0 or not out:
            return {"host": host, "ok": True, "tag": None, "out": "(no log found)"}
        path = out.strip()
    rc, out, err = ssh_run(host, f"tail -n {lines_n} {shlex.quote(path)} || true")
    return {"host": host, "ok": True, "tag": tag, "out": out, "err": err}
