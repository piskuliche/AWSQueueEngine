# Monitor loop and locking
import time
import fcntl
import os
import threading
from pathlib import Path
from .config import CHECK_INTERVAL
from .host_status import status_all
from .queue import dequeue_for_host, load_queue, save_queue, normalize_job_item
from .job_control import submit_to_host, write_run_info, kill_managed_on_host
from .running_state import load_running_jobs, save_running_jobs


def _host_is_eligible(job_item, host):
    hosts = job_item.get("hosts")
    return not hosts or host in hosts


def _requeue_front(job_item):
    q = load_queue()
    q.insert(0, normalize_job_item(job_item))
    save_queue(q)


def _launch_job_on_host(host, job_item, running_jobs):
    item = normalize_job_item(job_item)
    job_cmd = str(item.get("cmd") or "")
    payload = item.get("payload")
    payload_remote_path = item.get("payload_remote_path")
    target_hosts = item.get("hosts")
    priority = item.get("priority", 0)
    preempt = item.get("preempt", False)
    hosts_text = ",".join(target_hosts) if target_hosts else "any"
    print(
        f"[{time.strftime('%H:%M:%S')}] Launching (priority={priority}, hosts={hosts_text}, preempt={preempt}) on {host}: "
        f"{job_cmd[:120]}{'...' if len(job_cmd)>120 else ''}",
        flush=True,
    )
    res = submit_to_host(
        host,
        job_cmd,
        payload_local_path=payload,
        payload_remote_path=payload_remote_path,
    )
    if not res.get("ok"):
        print(f"  Failed to start on {host}: {res.get('err')}", flush=True)
        if res.get("err") != "pidfile present but process not running":
            _requeue_front(item)
        else:
            print(f"    Job Failed on {host}; but had pid file. Job maybe had error.", flush=True)
        return False

    remote_payload = res.get("payload") or payload_remote_path
    print(f"  Started {res.get('tag')} pid={res.get('pid')} payload={remote_payload or '-'}", flush=True)
    running_jobs[host] = {
        "cmd": job_cmd,
        "payload": payload,
        "priority": priority,
        "hosts": target_hosts,
        "preempt": False,
        "payload_remote_path": remote_payload,
        "started_at": time.time(),
    }
    save_running_jobs(running_jobs)
    write_run_info(
        local_payload_path=payload,
        jobid=res.get("tag"),
        host=host,
        remote_payload_path=remote_payload,
    )
    return True


def _select_preempt_target(queue_items, running_hosts, running_jobs):
    best_queue_idx = None
    best_item = None
    best_priority = None
    best_victim = None

    for idx, raw_item in enumerate(queue_items):
        item = normalize_job_item(raw_item)
        if not item.get("preempt"):
            continue
        eligible_hosts = [
            host
            for host in running_hosts
            if host in running_jobs and _host_is_eligible(item, host)
        ]
        if not eligible_hosts:
            continue

        def victim_sort_key(host):
            active = running_jobs.get(host, {})
            return (active.get("priority", 0), host)

        victim = sorted(eligible_hosts, key=victim_sort_key)[0]
        priority = item.get("priority", 0)
        if (
            best_queue_idx is None
            or priority > best_priority
            or (priority == best_priority and idx < best_queue_idx)
        ):
            best_queue_idx = idx
            best_item = item
            best_priority = priority
            best_victim = victim

    return best_queue_idx, best_item, best_victim


def _prune_running_jobs_for_status(running_jobs, status_rows):
    reachable_hosts = {row["host"] for row in status_rows if row.get("reachable")}
    active_hosts = {
        row["host"]
        for row in status_rows
        if row.get("reachable") and row.get("pid") is not None
    }
    changed = False
    for host in list(running_jobs):
        # Only drop metadata when the host is reachable and confirmed idle.
        if host in reachable_hosts and host not in active_hosts:
            running_jobs.pop(host, None)
            changed = True
    return changed


def monitor_loop(hosts, poll_interval=CHECK_INTERVAL, stop_event: threading.Event | None = None):
    print("Starting monitor loop. Press Ctrl-C to stop.", flush=True)

    if stop_event is None:
        stop_event = threading.Event()
    running_jobs = load_running_jobs()
    if running_jobs:
        print(f"Recovered {len(running_jobs)} running job record(s) from disk.", flush=True)
    try:
        while not stop_event.is_set():
            status = status_all(hosts)
            free_hosts = [s["host"] for s in status if s["reachable"] and s["pid"] is None]
            running_hosts = [s["host"] for s in status if s["reachable"] and s["pid"] is not None]
            unreachable_hosts = [s["host"] for s in status if not s["reachable"]]
            state_changed = _prune_running_jobs_for_status(running_jobs, status)
            if state_changed:
                save_running_jobs(running_jobs)
            if unreachable_hosts:
                print(f"[WARN] unreachable hosts: {', '.join(unreachable_hosts)}", flush=True)
            if free_hosts:
                for host in free_hosts:
                    job_item = dequeue_for_host(host)
                    if not job_item:
                        continue
                    _launch_job_on_host(host, job_item, running_jobs)

            queue_items = load_queue()
            preempt_queue_idx, preempt_item, victim_host = _select_preempt_target(
                queue_items,
                running_hosts,
                running_jobs,
            )
            if preempt_queue_idx is not None and preempt_item and victim_host:
                queue_items.pop(preempt_queue_idx)
                save_queue(queue_items)
                print(
                    f"[{time.strftime('%H:%M:%S')}] Preempting host {victim_host} for job: "
                    f"{str(preempt_item.get('cmd') or '')[:120]}",
                    flush=True,
                )
                interrupted_job = running_jobs.get(victim_host)
                kill_result = kill_managed_on_host(victim_host)
                if kill_result.get("rc") != 0:
                    print(
                        f"  Failed to preempt host {victim_host}: {kill_result.get('err') or kill_result.get('out')}",
                        flush=True,
                    )
                    _requeue_front(preempt_item)
                else:
                    running_jobs.pop(victim_host, None)
                    save_running_jobs(running_jobs)
                    if interrupted_job:
                        resume_item = normalize_job_item(interrupted_job)
                        resume_item["hosts"] = [victim_host]
                        resume_item["resume_first"] = True
                        resume_item["resume_host"] = victim_host
                        _requeue_front(resume_item)
                        print(
                            f"  Requeued interrupted job for {victim_host}: "
                            f"{str(resume_item.get('cmd') or '')[:120]}",
                            flush=True,
                        )
                    _launch_job_on_host(victim_host, preempt_item, running_jobs)
            stop_event.wait(poll_interval)
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
