# CLI interface for AWSQueueManager
import sys, os
import argparse
import shlex
import signal, threading
import subprocess
import tarfile
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path
from .config import HOSTS, HOSTS_FILE, S3_BUCKET, S3_PREFIX, SSH_BIN
from .queue import build_resume_item, enqueue_item, load_queue, normalize_job_item, save_queue
from .host_status import status_all
from .monitor import acquire_monitor_lock, load_hosts_from_file, release_monitor_lock, monitor_loop
from .job_control import submit_to_host, tail_remote_log, kill_managed_on_host
from .staging import sizeof_local_path_bytes, where_is_next_submit
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
    selected_hosts_file = hosts_file or HOSTS_FILE
    if not selected_hosts_file:
        return list(HOSTS)
    try:
        return load_hosts_from_file(selected_hosts_file)
    except OSError as exc:
        print(f"Failed to read hosts file {selected_hosts_file}: {exc}", flush=True)
        sys.exit(1)


def _parse_cli_host_values(host_values):
    hosts = []
    for host_value in host_values or []:
        if not host_value:
            continue
        hosts.extend(h.strip() for h in host_value.split(",") if h and h.strip())
    return list(dict.fromkeys(hosts))


def _normalize_host_set_name(host_set):
    if not isinstance(host_set, str):
        return None
    clean = host_set.strip()
    if not clean:
        return None
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in clean).strip("._-") or None


def _host_set_env_suffix(host_set):
    return _normalize_host_set_name(host_set).upper().replace("-", "_").replace(".", "_")


def _parse_hosts_text(hosts_text):
    return list(dict.fromkeys(h.strip() for h in hosts_text.replace(",", " ").split() if h.strip()))


def _resolve_host_set(host_set):
    clean_name = _normalize_host_set_name(host_set)
    if not clean_name:
        print("Host set name cannot be empty.", flush=True)
        sys.exit(1)
    suffix = _host_set_env_suffix(clean_name)
    inline_hosts = os.getenv(f"AWSQUEUEENGINE_HOST_SET_{suffix}", "").strip()
    hosts_file = os.getenv(f"AWSQUEUEENGINE_HOSTS_FILE_{suffix}", "").strip()
    if inline_hosts:
        hosts = _parse_hosts_text(inline_hosts)
    elif hosts_file:
        try:
            hosts = load_hosts_from_file(hosts_file)
        except OSError as exc:
            print(f"Failed to read host set {clean_name} file {hosts_file}: {exc}", flush=True)
            sys.exit(1)
    else:
        print(
            f"Unknown host set {clean_name!r}. Configure AWSQUEUEENGINE_HOST_SET_{suffix} "
            f"or AWSQUEUEENGINE_HOSTS_FILE_{suffix} on the queue host.",
            flush=True,
        )
        sys.exit(1)
    if not hosts:
        print(f"Host set {clean_name!r} is empty.", flush=True)
        sys.exit(1)
    return hosts


def _payload_display_text(item):
    return item.get("payload_remote_path") or item.get("payload_s3_uri") or item.get("payload") or "-"


def _archive_payload_to_temp(payload_path):
    payload = Path(payload_path).expanduser()
    if not payload.exists():
        raise FileNotFoundError(f"local payload not found: {payload}")
    tmp = tempfile.NamedTemporaryFile(prefix="awsqueueengine-payload-", suffix=".tar.gz", delete=False)
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        with tarfile.open(tmp_path, "w:gz") as tar:
            if payload.is_dir():
                children = list(payload.iterdir())
                if children:
                    for child in children:
                        tar.add(child, arcname=child.name)
                else:
                    tar.add(payload, arcname=".")
            else:
                tar.add(payload, arcname=payload.name)
    except Exception:
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise
    return tmp_path


def _upload_payload_archive_to_s3(archive_path, payload_name):
    if not S3_BUCKET:
        raise RuntimeError("AWSQUEUEENGINE_S3_BUCKET is required for remote submit with --payload.")
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for remote submit with --payload.") from exc

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    clean_name = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in payload_name) or "payload"
    key_parts = [part for part in (S3_PREFIX, f"{timestamp}-{uuid.uuid4().hex}", f"{clean_name}.tar.gz") if part]
    key = "/".join(key_parts)
    boto3.client("s3").upload_file(str(archive_path), S3_BUCKET, key)
    return f"s3://{S3_BUCKET}/{key}"


def _build_remote_submit_argv(args, command, payload_s3_uri=None, payload_size_bytes=None):
    argv = ["awsqueueengine", "submit"]
    if args.host_set:
        argv.extend(["--host-set", args.host_set])
    if args.hosts:
        for host_value in args.hosts:
            argv.extend(["--hosts", host_value])
    if args.priority is not None:
        argv.extend(["--priority", str(args.priority)])
    elif args.high_priority:
        argv.append("--high-priority")
    if args.preempt:
        argv.append("--preempt")
    if payload_s3_uri:
        argv.extend(["--payload-s3-uri", payload_s3_uri])
    if payload_size_bytes is not None:
        argv.extend(["--payload-size-bytes", str(payload_size_bytes)])
    argv.append(command)
    return argv


def _run_remote_submit(queue_host, remote_argv):
    remote_cmd = shlex.join(remote_argv)
    return subprocess.run([SSH_BIN, queue_host, remote_cmd], capture_output=True, text=True, check=False)


def _handle_remote_submit(args, command):
    if args.hosts_file:
        print("--hosts-file is not supported with --queue-host; host validation happens on the queue host.", flush=True)
        sys.exit(1)

    payload_s3_uri = None
    payload_size_bytes = None
    archive_path = None
    if args.payload:
        payload_path = Path(args.payload).expanduser()
        payload_size_bytes = sizeof_local_path_bytes(payload_path)
        try:
            archive_path = _archive_payload_to_temp(payload_path)
            payload_s3_uri = _upload_payload_archive_to_s3(archive_path, payload_path.name)
        except Exception as exc:
            print(f"Remote submit payload upload failed: {exc}", flush=True)
            sys.exit(1)
        finally:
            if archive_path:
                try:
                    archive_path.unlink()
                except OSError:
                    pass

    remote_argv = _build_remote_submit_argv(
        args,
        command,
        payload_s3_uri=payload_s3_uri,
        payload_size_bytes=payload_size_bytes,
    )
    result = _run_remote_submit(args.queue_host, remote_argv)
    if result.stdout:
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n", flush=True)
    if result.stderr:
        print(result.stderr, end="" if result.stderr.endswith("\n") else "\n", file=sys.stderr, flush=True)
    if result.returncode != 0:
        sys.exit(result.returncode)


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
    p_status.add_argument(
        "--host-set",
        help="Show status only for a named host set configured on this machine",
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
    p_submit.add_argument(
        "--host-set",
        help="Only run this job on a named host set configured on the queue host",
        default=None,
    )
    p_submit.add_argument("--priority", type=int, default=None, help="Integer priority (higher runs first)")
    p_submit.add_argument("--high-priority", action="store_true", help="Mark this job as high priority in the queue")
    p_submit.add_argument("--preempt", action="store_true", help="Allow this job to preempt a running managed job if needed")
    p_submit.add_argument("--queue-host", help="Forward this submit to a remote queue host over SSH", default=None)
    p_submit.add_argument("--payload-s3-uri", default=None, help=argparse.SUPPRESS)
    p_submit.add_argument("--payload-size-bytes", type=int, default=None, help=argparse.SUPPRESS)
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
        if args.host_set and args.hosts_file:
            print("--host-set and --hosts-file cannot be used together.", flush=True)
            sys.exit(1)
        monitor_hosts = _resolve_host_set(args.host_set) if args.host_set else _resolve_hosts_for_cli(args.hosts_file)
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
                payload_text = _payload_display_text(item)
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

        if args.queue_host and args.payload_s3_uri:
            print("--payload-s3-uri is only for queue-host-side submit handling.", flush=True)
            sys.exit(1)
        if args.host_set and args.hosts:
            print("--host-set and --hosts cannot be used together.", flush=True)
            sys.exit(1)

        command = " ".join(args.command).strip()
        if not command:
            print("No command provided.", flush=True)
            sys.exit(1)
        if args.queue_host:
            _handle_remote_submit(args, command)
            return

        if args.host_set and args.hosts_file:
            print("--host-set and --hosts-file cannot be used together.", flush=True)
            sys.exit(1)

        hosts = _resolve_host_set(args.host_set) if args.host_set else None
        valid_hosts = set(hosts or _resolve_hosts_for_cli(args.hosts_file))
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

        item = {
            "cmd": command,
            "payload": None if args.payload_s3_uri else args.payload,
            "priority": priority,
            "hosts": hosts,
            "preempt": args.preempt,
            "payload_s3_uri": args.payload_s3_uri,
            "payload_size_bytes": args.payload_size_bytes,
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
                payload_text = _payload_display_text(item)
                print(
                    f"{i:3d}. [priority={item['priority']}] [hosts={hosts_text}] [preempt={item['preempt']}] "
                    f"cmd={item['cmd']!r} payload={payload_text!r}",
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
            payload_text = _payload_display_text(item)
            print(
                f"  {idx:3d}. [priority={item['priority']}] [hosts={hosts_text}] [preempt={item['preempt']}] "
                f"cmd={item['cmd']!r} payload={payload_text!r}",
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
