# CLI interface for AWSQueueManager
import sys, os
import argparse
import signal, threading
import time
from datetime import datetime
from pathlib import Path
from .config import HOSTS
from .queue import build_resume_item, enqueue_item, load_queue, normalize_job_item, save_queue
from .host_status import status_all
from .monitor import acquire_monitor_lock, load_hosts_from_file, release_monitor_lock, monitor_loop
from .job_control import submit_to_host, tail_remote_log, kill_managed_on_host
from .staging import where_is_next_submit
from .running_state import load_running_jobs
from .notifications import parse_email_recipients, send_email


PIDFILE = Path.home() / "awsqueueengine.pid"

def write_pidfile():
    PIDFILE.write_text(str(os.getpid()))

def read_pidfile():
    if not PIDFILE.exists():
        return None
    try:
        return int(PIDFILE.read_text().strip())
    except Exception:
        return None
    
def remove_pidfile():
    try:
        PIDFILE.unlink()
    except Exception:
        pass

def pid_is_running(pid:int) -> bool:
    try:
        os.kill(pid, 0)  # does not kill; just checks
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True

stop_event = threading.Event()

def _handle_stop(signum, frame):
    print("Stopping monitor loop...")
    stop_event.set()


def _format_elapsed(started_at):
    if not isinstance(started_at, (int, float)):
        return "?"
    elapsed_seconds = max(0, int(time.time() - float(started_at)))
    hours, rem = divmod(elapsed_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _resolve_hosts_for_cli(hosts_file):
    if not hosts_file:
        return list(HOSTS)
    try:
        return load_hosts_from_file(hosts_file)
    except OSError as exc:
        print(f"Failed to read hosts file {hosts_file}: {exc}", flush=True)
        sys.exit(1)


def _parse_cli_host_values(host_values):
    hosts = []
    for host_value in host_values or []:
        if not host_value:
            continue
        hosts.extend(h.strip() for h in host_value.split(",") if h and h.strip())
    return list(dict.fromkeys(hosts))


def main():

    # Set unbuffered output for stdout and stderr
    sys.stdout = open(sys.stdout.fileno(), mode='w', buffering=1, encoding=sys.stdout.encoding, closefd=False)
    sys.stderr = open(sys.stderr.fileno(), mode='w', buffering=1, encoding=sys.stderr.encoding, closefd=False)

    parser = argparse.ArgumentParser(description="Simple Slurm-like manager for SSH GPU hosts.")
    parser.add_argument(
        "--test-email-connection",
        action="store_true",
        help="Send a test notification email through Mailtrap and exit.",
    )
    sub = parser.add_subparsers(dest="cmd")

    p_status = sub.add_parser("status", help="Show status for all hosts")
    p_status.add_argument(
        "--hosts-file",
        help="Optional file with hostnames (comma or whitespace separated).",
        default=None,
    )
    sub.add_parser("qstat", help="Show running jobs tracked by monitor")
    p_submit = sub.add_parser("submit", help="Enqueue a job (command string)")
    p_submit.add_argument(
        "--hosts-file",
        help="Optional file with valid hostnames (comma or whitespace separated).",
        default=None,
    )
    p_submit.add_argument("--payload", "-p", help="Local folder to copy to remote scratch before running", default=None)
    p_submit.add_argument(
        "--hosts",
        action="append",
        metavar="HOST",
        help="Only run this job on listed host(s). Repeat flag or use comma-separated values.",
        default=None,
    )
    p_submit.add_argument("--priority", type=int, default=None, help="Integer priority (higher runs first)")
    p_submit.add_argument("--high-priority", action="store_true", help="Mark this job as high priority in the queue")
    p_submit.add_argument("--preempt", action="store_true", help="Allow this job to preempt a running managed job if needed")
    p_submit.add_argument("command", nargs=argparse.REMAINDER, help="Command to run remotely (quoted)")
    p_requeue = sub.add_parser(
        "requeue-running",
        help="Kill running managed job(s) and requeue them to the same host at priority 100",
    )
    p_requeue.add_argument(
        "--hosts-file",
        help="Optional file with valid hostnames (comma or whitespace separated).",
        default=None,
    )
    p_requeue_target = p_requeue.add_mutually_exclusive_group(required=True)
    p_requeue_target.add_argument(
        "--hosts",
        action="append",
        metavar="HOST",
        help="Requeue running job(s) only on listed host(s). Repeat flag or use comma-separated values.",
        default=None,
    )
    p_requeue_target.add_argument(
        "--all",
        "-all",
        action="store_true",
        help="Requeue running job(s) on all hosts with tracked running jobs.",
    )

    sub.add_parser("list", help="Show queued jobs")
    p_qdel = sub.add_parser("qdel", help="Delete queued job(s) by list index")
    p_qdel.add_argument("job_ids", nargs="+", type=int, help="1-based queue index(es) from `list` output")
    sub.add_parser("clear", help="Clear the queue")
    sub.add_parser("start", help="Start monitor loop (runs until Ctrl-C)")
    sub.add_parser("where", help="Show where the next job will be submitted")
    p_start_monitor = sub.add_parser("start-monitor", help="Start the monitor loop (daemon mode)")
    p_start_monitor.add_argument(
        "--hosts-file",
        help="Optional file with hostnames to monitor (reloaded while running).",
        default=None,
    )
    sub.add_parser("stop-monitor", help="Stop the running monitor loop")
    sub.add_parser("status-monitor", help="Show monitor status")

    p_tail = sub.add_parser("tail", help="Tail remote log on a host")
    p_tail.add_argument("host")

    p_stop = sub.add_parser("stop", help="Kill managed job(s) on a host")
    p_stop.add_argument("host")

    args = parser.parse_args()
    print("Starting the queue engine", flush=True)

    if args.test_email_connection:
        recipients = parse_email_recipients()
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result = send_email(
            subject=f"[AWSQueueEngine] Test email ({now_text})",
            body=f"This is a test email from AWSQueueEngine.\nGenerated at: {now_text}",
            recipients=recipients,
        )
        if result.get("skipped"):
            print("Email test skipped (Mailtrap not configured or no recipients).", flush=True)
            return
        if result.get("ok"):
            print("Test email sent.", flush=True)
            return
        print(f"Email test failed: {result.get('err')}", flush=True)
        sys.exit(1)

    if args.cmd == "status":
        monitor_hosts = _resolve_hosts_for_cli(args.hosts_file)
        rows = status_all(monitor_hosts)
        print(f"{'HOST':8}  {'REACH':8}  {'PID':8}  {'TAG':12}  INFO", flush=True)
        for r in rows:
            reach = "yes" if r["reachable"] else "no"
            pid = r["pid"] or "-"
            tag = r["tag"] or "-"
            info = (r["raw"][:60] + "...") if r["raw"] else ""
            print(f"{r['host']:8}  {reach:8}  {pid:8}  {tag:12}  {info}", flush=True)
    elif args.cmd == "qstat":
        running_jobs = load_running_jobs()
        if not running_jobs:
            print("(no running jobs tracked)", flush=True)
        else:
            print(f"{'HOST':8}  {'DUR':8}  {'PRI':5}  {'PREEMPT':7}  {'HOSTS':15}  {'PAYLOAD':24}  CMD", flush=True)
            for host in sorted(running_jobs):
                item = running_jobs[host]
                hosts_text = ",".join(item["hosts"]) if item["hosts"] else "any"
                payload_text = item.get("payload_remote_path") or item.get("payload") or "-"
                cmd_text = str(item.get("cmd") or "")
                dur_text = _format_elapsed(item.get("started_at"))
                print(
                    f"{host:8}  {dur_text:8}  {item['priority']:5d}  {str(item['preempt']):7}  {hosts_text[:15]:15}  {payload_text:24}  "
                    f"{cmd_text}",
                    flush=True,
                )
    elif args.cmd == "submit":
        if not args.command:
            print("No command provided.", flush=True)
            sys.exit(1)

        valid_hosts = set(_resolve_hosts_for_cli(args.hosts_file))
        hosts = None
        if args.hosts:
            requested_hosts = _parse_cli_host_values(args.hosts)
            invalid_hosts = sorted({h for h in requested_hosts if h not in valid_hosts})
            if invalid_hosts:
                valid_hosts_text = ", ".join(sorted(valid_hosts)) if valid_hosts else "(none)"
                print(f"Invalid host(s): {', '.join(invalid_hosts)}. Valid hosts: {valid_hosts_text}", flush=True)
                sys.exit(1)
            hosts = list(dict.fromkeys(requested_hosts))

        if args.priority is not None:
            priority = args.priority
        elif args.high_priority:
            priority = 100
        else:
            priority = 0

        command = " ".join(args.command).strip()
        if not command:
            print("No command provided.", flush=True)
            sys.exit(1)
        item = {
            "cmd": command,
            "payload": args.payload,
            "priority": priority,
            "hosts": hosts,
            "preempt": args.preempt,
        }
        enqueue_item(item)
        print("Enqueued:", item, flush=True)
    elif args.cmd == "requeue-running":
        running_jobs = load_running_jobs()
        valid_hosts = set(_resolve_hosts_for_cli(args.hosts_file))
        if args.all:
            target_hosts = [host for host in sorted(running_jobs) if host in valid_hosts]
        else:
            requested_hosts = _parse_cli_host_values(args.hosts)
            invalid_hosts = sorted({h for h in requested_hosts if h not in valid_hosts})
            if invalid_hosts:
                valid_hosts_text = ", ".join(sorted(valid_hosts)) if valid_hosts else "(none)"
                print(f"Invalid host(s): {', '.join(invalid_hosts)}. Valid hosts: {valid_hosts_text}", flush=True)
                sys.exit(1)
            target_hosts = requested_hosts

        if not target_hosts:
            print("No target hosts found for requeue-running.", flush=True)
            return

        requeued_count = 0
        for host in target_hosts:
            running_item = running_jobs.get(host)
            if not running_item:
                print(f"No tracked running job on {host}; skipping requeue.", flush=True)
                continue

            resume_item = build_resume_item(running_item, host, priority=100)
            q = load_queue()
            q.insert(0, resume_item)
            save_queue(q)
            requeued_count += 1
            print(
                f"Requeued running job for {host} at priority 100: "
                f"{str(resume_item.get('cmd') or '')[:120]}",
                flush=True,
            )

            res = kill_managed_on_host(host)
            if res["rc"] == 0:
                print(f"Sent kill to managed job(s) on {host}.", flush=True)
            else:
                detail = res.get("err") or res.get("out") or "(no stderr/stdout returned)"
                print(f"Kill error on {host} (rc={res['rc']}): {detail}", flush=True)

        if requeued_count == 0:
            print("No running jobs were requeued.", flush=True)
    elif args.cmd == "list":
        q = load_queue()
        if not q:
            print("(queue empty)", flush=True)
        else:
            for i, raw_item in enumerate(q, 1):
                item = normalize_job_item(raw_item)
                hosts_text = ",".join(item["hosts"]) if item["hosts"] else "any"
                print(
                    f"{i:3d}. [priority={item['priority']}] [hosts={hosts_text}] [preempt={item['preempt']}] "
                    f"cmd={item['cmd']!r} payload={item['payload']!r}",
                    flush=True
                )
    elif args.cmd == "qdel":
        q = load_queue()
        if not q:
            print("(queue empty)", flush=True)
            sys.exit(1)

        queue_size = len(q)
        unique_ids = sorted(set(args.job_ids))
        invalid_ids = [idx for idx in unique_ids if idx < 1 or idx > queue_size]
        if invalid_ids:
            print(
                f"Invalid queue index(es): {', '.join(str(i) for i in invalid_ids)}. "
                f"Queue size: {queue_size}",
                flush=True,
            )
            sys.exit(1)

        removed_jobs = []
        for idx in sorted(unique_ids, reverse=True):
            removed = normalize_job_item(q.pop(idx - 1))
            removed_jobs.append((idx, removed))
        save_queue(q)

        print(f"Removed {len(removed_jobs)} job(s).", flush=True)
        for idx, item in sorted(removed_jobs, key=lambda pair: pair[0]):
            hosts_text = ",".join(item["hosts"]) if item["hosts"] else "any"
            print(
                f"  {idx:3d}. [priority={item['priority']}] [hosts={hosts_text}] [preempt={item['preempt']}] "
                f"cmd={item['cmd']!r} payload={item['payload']!r}",
                flush=True,
            )
    elif args.cmd == "clear":
        save_queue([])
        print("Queue cleared.", flush=True)
    elif args.cmd == "start-monitor":
        # Prevent double-start
        pid = read_pidfile()
        if pid and pid_is_running(pid):
            print(f"Monitor already running (pid={pid})", flush=True)
            sys.exit(1)
        else:
            remove_pidfile()

        signal.signal(signal.SIGTERM, _handle_stop)
        signal.signal(signal.SIGINT, _handle_stop)

        fd, holder = acquire_monitor_lock()
        if fd is None:
            print(f"Monitor already running (holder={holder})", flush=True)
            sys.exit(1)

        write_pidfile()
        print(f"Monitor started (pid={os.getpid()})", flush=True)

        monitor_hosts = _resolve_hosts_for_cli(args.hosts_file)
        try:
            monitor_loop(monitor_hosts, stop_event=stop_event, hosts_file=args.hosts_file)
            print("Monitor exited cleanly.", flush=True)
        finally:
            remove_pidfile()
            release_monitor_lock(fd)
    elif args.cmd == "stop-monitor":
        pid = read_pidfile()
        if not pid:
            print("Monitor not running (no pidfile).", flush=True)
            sys.exit(1)

        if not pid_is_running(pid):
            print(f"Stale pidfile found (pid={pid}); cleaning up.", flush=True)
            remove_pidfile()
            sys.exit(1)

        print(f"Stopping monitor (pid={pid})...", flush=True)
        os.kill(pid, signal.SIGTERM)
    elif args.cmd == "status-monitor":
        pid = read_pidfile()
        if not pid:
            print("Monitor not running.", flush=True)
            return

        if pid_is_running(pid):
            print(f"Monitor running (pid={pid})", flush=True)
        else:
            print(f"Monitor NOT running (stale pidfile pid={pid})", flush=True)
    elif args.cmd == "tail":
        r = tail_remote_log(args.host)
        if not r["ok"]:
            print("Error:", r.get("reason") or r.get("err"), flush=True)
        else:
            header = f"Host: {r['host']}  tag: {r.get('tag') or '(none)'}"
            print(header, flush=True)
            print("-"*len(header), flush=True)
            print(r.get("out") or "(no log output)", flush=True)
    elif args.cmd == "stop":
        res = kill_managed_on_host(args.host)
        if res["rc"] == 0:
            print(f"Sent kill to managed job(s) on {args.host}.", flush=True)
        else:
            detail = res.get("err") or res.get("out") or "(no stderr/stdout returned)"
            print(f"Kill error (rc={res['rc']}): {detail}", flush=True)
    elif args.cmd == "where":
        where_is_next_submit()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
