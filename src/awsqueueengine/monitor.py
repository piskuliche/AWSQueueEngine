# Monitor loop and locking
import time
import fcntl
import os
from pathlib import Path
from .config import CHECK_INTERVAL, HOSTS
from .host_status import status_all
from .queue import dequeue, load_queue, save_queue
from .job_control import submit_to_host, write_run_info

def monitor_loop(hosts, poll_interval=CHECK_INTERVAL):
    print("Starting monitor loop. Press Ctrl-C to stop.", flush=True)
    try:
        while True:
            status = status_all(hosts)
            free_hosts = [s["host"] for s in status if s["reachable"] and s["pid"] is None]
            unreachable_hosts = [s["host"] for s in status if not s["reachable"]]
            if unreachable_hosts:
                print(f"[WARN] unreachable hosts: {', '.join(unreachable_hosts)}", flush=True)
            if free_hosts:
                for host in free_hosts:
                    job_item = dequeue()
                    if not job_item:
                        break
                    if isinstance(job_item, str):
                        job_cmd = job_item
                        payload = None
                    else:
                        job_cmd = job_item.get("cmd")
                        payload = job_item.get("payload")
                    print(f"[{time.strftime('%H:%M:%S')}] Launching on {host}: {job_cmd[:120]}{'...' if len(job_cmd)>120 else ''}", flush=True)
                    res = submit_to_host(host, job_cmd, payload_local_path=payload)
                    if not res.get("ok"):
                        print(f"  Failed to start on {host}: {res.get('err')}", flush=True)
                        if not res.get('err')=="pidfile present but process not running":
                            q = load_queue()
                            q.insert(0, job_item)
                            save_queue(q)
                        else:
                            print(f"    Job Failed on {host}; but had pid file. Job maybe had error.", flush=True)
                    else:
                        print(f"  Started {res.get('tag')} pid={res.get('pid')} payload={res.get('payload', '-')}", flush=True)
                        tag = res.get("tag")
                        remote_payload = res.get("payload")
                        write_run_info(
                            local_payload_path=payload,
                            jobid=tag,
                            host=host,
                            remote_payload_path=remote_payload
                        )
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        print("\nMonitor stopped by user.", flush=True)
    except Exception as e:
        print("Monitor loop error:", e, flush=True)

def acquire_monitor_lock(lock_path=Path.home() / ".aws_slurm_like.lock"):
    lock_path = Path(lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = open(str(lock_path), "a+")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        try:
            fd.seek(0)
            holder = fd.read().strip() or None
        except Exception:
            holder = None
        fd.close()
        return None, holder or "locked"
    try:
        fd.seek(0)
        fd.truncate()
        fd.write(str(os.getpid()) + "\n")
        fd.flush()
    except Exception:
        pass
    return fd, None

def release_monitor_lock(fd):
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        fd.close()
    except Exception:
        pass
