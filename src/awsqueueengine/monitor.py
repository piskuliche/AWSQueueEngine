# Monitor loop and locking
import json
import time
import fcntl
import os
import threading
from datetime import datetime
from pathlib import Path
from .config import (
    ALERT_DAILY_EMAIL_LIMIT,
    CHECK_INTERVAL,
    JOB_FAIL_ALERT_COOLDOWN_SECONDS,
    MAX_SUBMIT_FAILURES,
    MONITOR_STATE_FILE,
)
from .host_status import status_all
from .queue import dequeue_for_host, load_queue, save_queue, normalize_job_item
from .job_control import submit_to_host, write_run_info, kill_managed_on_host
from .running_state import load_running_jobs, save_running_jobs
from .completion_state import append_completed_records
from .notifications import parse_email_recipients, send_email


def _normalize_hosts(host_values):
    normalized_hosts = []
    seen = set()
    for host in host_values or []:
        if not isinstance(host, str):
            continue
        clean_host = host.strip()
        if not clean_host or clean_host in seen:
            continue
        normalized_hosts.append(clean_host)
        seen.add(clean_host)
    return normalized_hosts


def load_hosts_from_file(hosts_file):
    hosts_path = Path(hosts_file).expanduser()
    raw_text = hosts_path.read_text()
    parsed_hosts = []
    for raw_line in raw_text.splitlines():
        # Strip inline comments and support both comma/whitespace separators.
        line = raw_line.split("#", 1)[0].replace(",", " ")
        parsed_hosts.extend(line.split())
    return _normalize_hosts(parsed_hosts)


class _MonitorHostSource:
    def __init__(self, hosts, hosts_file=None):
        self._hosts = _normalize_hosts(hosts)
        self._hosts_file = Path(hosts_file).expanduser() if hosts_file else None
        self._last_file_stamp = None
        self._missing_file_warned = False
        if self._hosts_file:
            self.refresh(force=True)

    @property
    def hosts_file(self):
        return self._hosts_file

    def _compute_file_stamp(self):
        if not self._hosts_file:
            return None
        try:
            stat_result = self._hosts_file.stat()
        except FileNotFoundError:
            return None
        return (stat_result.st_mtime_ns, stat_result.st_size)

    def refresh(self, force=False):
        if not self._hosts_file:
            return list(self._hosts)

        file_stamp = self._compute_file_stamp()
        if file_stamp is None:
            if not self._missing_file_warned:
                print(
                    f"[WARN] Hosts file not found: {self._hosts_file}. "
                    f"Keeping previous host list ({len(self._hosts)} host(s)).",
                    flush=True,
                )
                self._missing_file_warned = True
            self._last_file_stamp = None
            return list(self._hosts)

        if not force and self._last_file_stamp == file_stamp:
            return list(self._hosts)

        try:
            next_hosts = load_hosts_from_file(self._hosts_file)
        except OSError as exc:
            print(
                f"[WARN] Failed to read hosts file {self._hosts_file}: {exc}. "
                f"Keeping previous host list ({len(self._hosts)} host(s)).",
                flush=True,
            )
            self._last_file_stamp = file_stamp
            return list(self._hosts)

        self._missing_file_warned = False
        if next_hosts != self._hosts:
            previous_hosts = set(self._hosts)
            next_host_set = set(next_hosts)
            added = [host for host in next_hosts if host not in previous_hosts]
            removed = [host for host in self._hosts if host not in next_host_set]
            added_text = ", ".join(added) if added else "-"
            removed_text = ", ".join(removed) if removed else "-"
            print(
                f"[INFO] Hosts updated from {self._hosts_file}: {len(next_hosts)} host(s) "
                f"(added: {added_text}; removed: {removed_text})",
                flush=True,
            )
            self._hosts = next_hosts

        self._last_file_stamp = file_stamp
        return list(self._hosts)


def _host_is_eligible(job_item, host):
    hosts = job_item.get("hosts")
    return not hosts or host in hosts


def _requeue_front(job_item):
    q = load_queue()
    q.insert(0, normalize_job_item(job_item))
    save_queue(q)


def _build_status_text(status_rows, running_jobs, queue_items):
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_hosts = len(status_rows)
    reachable = sum(1 for row in status_rows if row.get("reachable"))
    unreachable = total_hosts - reachable
    running = sum(1 for row in status_rows if row.get("reachable") and row.get("pid") is not None)
    free = sum(1 for row in status_rows if row.get("reachable") and row.get("pid") is None)
    lines = [
        f"Generated at: {now_text}",
        f"Queue depth: {len(queue_items)}",
        f"Running jobs tracked: {len(running_jobs)}",
        f"Hosts: total={total_hosts} reachable={reachable} free={free} running={running} unreachable={unreachable}",
    ]
    if queue_items:
        next_item = normalize_job_item(queue_items[0])
        lines.append(f"Next queued command: {str(next_item.get('cmd') or '')[:180]}")
    return "\n".join(lines)


def _format_duration_seconds(duration_seconds):
    duration_int = max(0, int(duration_seconds))
    hours, rem = divmod(duration_int, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _build_completed_job_record(host, running_item, finished_at):
    item = normalize_job_item(running_item)
    started_at = running_item.get("started_at") if isinstance(running_item, dict) else None
    if isinstance(started_at, (int, float)):
        duration_seconds = max(0, int(finished_at - float(started_at)))
    else:
        duration_seconds = 0
        started_at = None
    payload_text = item.get("payload_remote_path") or item.get("payload") or "-"
    return {
        "host": host,
        "dur": _format_duration_seconds(duration_seconds),
        "duration_seconds": duration_seconds,
        "priority": item.get("priority", 0),
        "preempt": item.get("preempt", False),
        "hosts": item.get("hosts"),
        "payload": payload_text,
        "cmd": str(item.get("cmd") or ""),
        "started_at": started_at,
        "finished_at": float(finished_at),
    }


def _should_send_low_queue_alert(queue_len, already_sent):
    return 0 < queue_len < 10 and not already_sent


def _should_send_empty_queue_alert(queue_len, already_sent):
    return queue_len == 0 and not already_sent


def _should_send_daily_summary(last_summary_date, now_dt):
    return last_summary_date != now_dt.date()


def _reset_queue_alert_state(queue_len, low_queue_alert_sent, empty_queue_alert_sent):
    # Reset both queue alert latches only after queue recovers to healthy depth.
    if queue_len >= 10:
        return False, False
    return low_queue_alert_sent, empty_queue_alert_sent


def _send_alert_email(recipients, subject, body):
    result = send_email(subject, body, recipients)
    if result.get("skipped"):
        return True
    if result.get("ok"):
        print(f"[EMAIL] {subject}", flush=True)
        return True
    print(f"[EMAIL-ERROR] {subject}: {result.get('err')}", flush=True)
    return False


def _load_monitor_state():
    if not MONITOR_STATE_FILE.exists():
        return {}
    try:
        data = json.loads(MONITOR_STATE_FILE.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_monitor_state(state):
    try:
        MONITOR_STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as exc:
        print(f"[WARN] Could not save monitor state: {exc}", flush=True)


def _load_last_daily_summary_date():
    state = _load_monitor_state()
    text = state.get("last_daily_summary_date")
    if not isinstance(text, str) or not text.strip():
        return None
    try:
        return datetime.strptime(text.strip(), "%Y-%m-%d").date()
    except Exception:
        return None


def _save_last_daily_summary_date(summary_date):
    state = _load_monitor_state()
    state["last_daily_summary_date"] = summary_date.strftime("%Y-%m-%d")
    _save_monitor_state(state)


def _initial_alert_runtime_state():
    return {
        "day": datetime.now().date(),
        "sent_today": 0,
        "daily_limit_warned": False,
        "job_fail_cooldown_until": 0.0,
        "job_fail_suppressed_count": 0,
    }


def _reset_daily_alert_state_if_needed(alert_state, now_dt):
    if alert_state.get("day") == now_dt.date():
        return
    alert_state["day"] = now_dt.date()
    alert_state["sent_today"] = 0
    alert_state["daily_limit_warned"] = False
    alert_state["job_fail_suppressed_count"] = 0
    alert_state["job_fail_cooldown_until"] = 0.0


def _should_send_alert(alert_state, alert_type, now_dt, now_ts):
    _reset_daily_alert_state_if_needed(alert_state, now_dt)
    if alert_state["sent_today"] >= ALERT_DAILY_EMAIL_LIMIT:
        if not alert_state["daily_limit_warned"]:
            print(
                f"[EMAIL-WARN] Daily email limit reached ({ALERT_DAILY_EMAIL_LIMIT}); suppressing alerts until tomorrow.",
                flush=True,
            )
            alert_state["daily_limit_warned"] = True
        if alert_type == "job_fail":
            alert_state["job_fail_suppressed_count"] += 1
        return False
    if alert_type == "job_fail" and now_ts < alert_state["job_fail_cooldown_until"]:
        alert_state["job_fail_suppressed_count"] += 1
        return False
    return True


def _build_job_fail_alert_body(base_body, alert_state):
    suppressed_count = int(alert_state.get("job_fail_suppressed_count") or 0)
    if suppressed_count <= 0:
        return base_body
    alert_state["job_fail_suppressed_count"] = 0
    return (
        f"{base_body}\n\n"
        f"Suppressed {suppressed_count} additional job start-failure email(s) during cooldown."
    )


def _mark_alert_sent(alert_state, alert_type, now_ts):
    alert_state["sent_today"] += 1
    if alert_type == "job_fail":
        alert_state["job_fail_cooldown_until"] = now_ts + JOB_FAIL_ALERT_COOLDOWN_SECONDS


def _send_alert_email_with_limits(recipients, subject, body, alert_state, alert_type):
    now_dt = datetime.now()
    now_ts = time.time()
    if not _should_send_alert(alert_state, alert_type, now_dt, now_ts):
        return True
    if alert_type == "job_fail":
        body = _build_job_fail_alert_body(body, alert_state)
    sent = _send_alert_email(recipients, subject, body)
    if sent:
        _mark_alert_sent(alert_state, alert_type, now_ts)
    return sent


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
            failures = item.get("submit_failures", 0) + 1
            item["submit_failures"] = failures
            if failures >= MAX_SUBMIT_FAILURES:
                print(
                    f"  Job exceeded max submit failures ({MAX_SUBMIT_FAILURES}); dropping from queue: "
                    f"{str(item.get('cmd') or '')[:120]}",
                    flush=True,
                )
            else:
                print(f"  Requeuing job (failure {failures}/{MAX_SUBMIT_FAILURES}).", flush=True)
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
    completed_records = []
    finished_at = time.time()
    for host in list(running_jobs):
        # Only drop metadata when the host is reachable and confirmed idle.
        if host in reachable_hosts and host not in active_hosts:
            finished_item = running_jobs.pop(host, None)
            if isinstance(finished_item, dict):
                completed_records.append(_build_completed_job_record(host, finished_item, finished_at))
            changed = True
    return changed, completed_records


def monitor_loop(hosts, poll_interval=CHECK_INTERVAL, stop_event: threading.Event | None = None, hosts_file=None):
    print("Starting monitor loop. Press Ctrl-C to stop.", flush=True)

    if stop_event is None:
        stop_event = threading.Event()
    host_source = _MonitorHostSource(hosts, hosts_file=hosts_file)
    if host_source.hosts_file:
        print(f"Monitoring hosts from file: {host_source.hosts_file}", flush=True)

    running_jobs = load_running_jobs()
    alert_recipients = parse_email_recipients()
    alert_state = _initial_alert_runtime_state()
    low_queue_alert_sent = False
    empty_queue_alert_sent = False
    last_daily_summary_date = _load_last_daily_summary_date()
    if last_daily_summary_date is None:
        last_daily_summary_date = datetime.now().date()
        _save_last_daily_summary_date(last_daily_summary_date)
    if running_jobs:
        print(f"Recovered {len(running_jobs)} running job record(s) from disk.", flush=True)
    try:
        no_hosts_warned = False
        while not stop_event.is_set():
            current_hosts = host_source.refresh()
            if not current_hosts:
                if not no_hosts_warned:
                    print("[WARN] No eligible hosts configured; monitor will wait for hosts.", flush=True)
                    no_hosts_warned = True
            else:
                no_hosts_warned = False

            status = status_all(current_hosts)
            free_hosts = [s["host"] for s in status if s["reachable"] and s["pid"] is None]
            running_hosts = [s["host"] for s in status if s["reachable"] and s["pid"] is not None]
            unreachable_hosts = [s["host"] for s in status if not s["reachable"]]
            state_changed, completed_records = _prune_running_jobs_for_status(running_jobs, status)
            if state_changed:
                save_running_jobs(running_jobs)
            if completed_records:
                append_completed_records(completed_records)
            if unreachable_hosts:
                print(f"[WARN] unreachable hosts: {', '.join(unreachable_hosts)}", flush=True)
            if free_hosts:
                for host in free_hosts:
                    job_item = dequeue_for_host(host)
                    if not job_item:
                        continue
                    launched = _launch_job_on_host(host, job_item, running_jobs)
                    if not launched:
                        item = normalize_job_item(job_item)
                        alert_body = (
                            f"Failed to start queued job on host {host}.\n\n"
                            f"Command: {item.get('cmd')}\n"
                            f"Priority: {item.get('priority')}\n"
                            f"Hosts restriction: {item.get('hosts') or 'any'}\n"
                            f"Payload dir: {item.get('payload') or '-'}\n"
                            f"Payload remote path: {item.get('payload_remote_path') or '-'}\n"
                            f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        )
                        _send_alert_email_with_limits(
                            alert_recipients,
                            f"[AWSQueueEngine] Job failed to start on {host}",
                            alert_body,
                            alert_state,
                            "job_fail",
                        )
                        break  # avoid retrying the same requeued job on other free hosts this cycle

            queue_items = load_queue()
            queue_len = len(queue_items)
            low_queue_alert_sent, empty_queue_alert_sent = _reset_queue_alert_state(
                queue_len,
                low_queue_alert_sent,
                empty_queue_alert_sent,
            )
            if _should_send_low_queue_alert(queue_len, low_queue_alert_sent):
                summary = _build_status_text(status, running_jobs, queue_items)
                if _send_alert_email_with_limits(
                    alert_recipients,
                    "[AWSQueueEngine] Queue running low (<10 jobs remaining)",
                    summary,
                    alert_state,
                    "queue_low",
                ):
                    low_queue_alert_sent = True
            if _should_send_empty_queue_alert(queue_len, empty_queue_alert_sent):
                summary = _build_status_text(status, running_jobs, queue_items)
                if _send_alert_email_with_limits(
                    alert_recipients,
                    "[AWSQueueEngine] Queue is empty",
                    summary,
                    alert_state,
                    "queue_empty",
                ):
                    empty_queue_alert_sent = True
                    low_queue_alert_sent = True

            now_dt = datetime.now()
            if _should_send_daily_summary(last_daily_summary_date, now_dt):
                summary = _build_status_text(status, running_jobs, queue_items)
                if _send_alert_email_with_limits(
                    alert_recipients,
                    f"[AWSQueueEngine] Daily summary ({now_dt.strftime('%Y-%m-%d')})",
                    summary,
                    alert_state,
                    "daily_summary",
                ):
                    last_daily_summary_date = now_dt.date()
                    _save_last_daily_summary_date(last_daily_summary_date)

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
