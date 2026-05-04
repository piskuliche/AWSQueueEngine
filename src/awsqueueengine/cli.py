# CLI interface for AWSQueueManager
import json
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
from .queue_config import DEFAULT_QUEUE, QueueConfigSource, get_configured_queue_source, normalize_queue_name
from .host_status import status_all
from .monitor import acquire_monitor_lock, load_hosts_from_file, release_monitor_lock, monitor_loop
from .job_control import new_job_tag, submit_to_host, tail_remote_log, kill_managed_on_host
from .staging import sizeof_local_path_bytes, where_is_next_submit
from .running_state import load_running_jobs
from .completion_state import load_completed_jobs
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


def _resolve_queue_hosts_for_cli(hosts_file=None):
    try:
        source_kind, _source_value = get_configured_queue_source()
        legacy_hosts = HOSTS if source_kind else _resolve_hosts_for_cli(hosts_file)
        source = QueueConfigSource(legacy_hosts=legacy_hosts, legacy_hosts_file=hosts_file or HOSTS_FILE or None)
        return source.refresh()
    except ValueError as exc:
        print(f"Invalid queue host configuration: {exc}", flush=True)
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


def _build_remote_submit_argv(args, command, payload_s3_uri=None, payload_size_bytes=None, job_id=None):
    argv = ["awsqueueengine", "submit"]
    queue_name = getattr(args, "queue", None) or getattr(args, "host_set", None)
    if queue_name:
        argv.extend(["--queue", queue_name])
    if getattr(args, "hosts", None):
        for host_value in args.hosts:
            argv.extend(["--hosts", host_value])
    if args.priority is not None:
        argv.extend(["--priority", str(args.priority)])
    elif args.high_priority:
        argv.append("--high-priority")
    if getattr(args, "preempt", False):
        argv.append("--preempt")
    if payload_s3_uri:
        argv.extend(["--payload-s3-uri", payload_s3_uri])
    if payload_size_bytes is not None:
        argv.extend(["--payload-size-bytes", str(payload_size_bytes)])
    if job_id:
        argv.extend(["--job-id", job_id])
    argv.append(command)
    return argv


def _run_remote_submit(queue_host, remote_argv):
    remote_cmd = shlex.join(remote_argv)
    return subprocess.run([SSH_BIN, queue_host, remote_cmd], capture_output=True, text=True, check=False)


def _proxy_remote_cli(queue_host, remote_argv):
    """Run an awsqueueengine CLI command on a remote queue host and stream its output."""
    remote_cmd = shlex.join(remote_argv)
    result = subprocess.run([SSH_BIN, queue_host, remote_cmd], capture_output=True, text=True, check=False)
    if result.stdout:
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n", flush=True)
    if result.stderr:
        print(result.stderr, end="" if result.stderr.endswith("\n") else "\n", file=sys.stderr, flush=True)
    if result.returncode != 0:
        sys.exit(result.returncode)


def _write_local_run_info(payload_path, info):
    """Write a run.info file in the user's local payload dir with submit metadata."""
    if not payload_path:
        return None
    target_dir = Path(payload_path).expanduser()
    if not target_dir.exists() or not target_dir.is_dir():
        return None
    info_path = target_dir / "run.info"
    lines = []
    for key, value in info.items():
        if value is None or value == "":
            continue
        lines.append(f"{key}: {value}")
    try:
        info_path.write_text("\n".join(lines) + "\n")
    except OSError as exc:
        print(f"[WARN] Could not write {info_path}: {exc}", flush=True)
        return None
    return info_path


def _read_run_info_file(info_path):
    info = {}
    for line in info_path.read_text().splitlines():
        if ":" not in line:
            continue
        key, _, value = line.partition(":")
        clean_key = key.strip()
        if not clean_key:
            continue
        info[clean_key] = value.strip()
    return info


def _write_run_info_file(info_path, info):
    lines = []
    for key, value in info.items():
        if value is None or value == "":
            continue
        lines.append(f"{key}: {value}")
    info_path.write_text("\n".join(lines) + "\n")


def _format_epoch(value):
    if isinstance(value, bool):
        return ""
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value)).strftime("%Y-%m-%d %H:%M:%S")
        except (OverflowError, OSError, ValueError):
            return ""
    return ""


def lookup_job_state(job_id):
    """Search local queue/running/completed state for a job_id and return a state dict."""
    if not job_id:
        return None
    for idx, raw_item in enumerate(load_queue(), 1):
        item = normalize_job_item(raw_item)
        if item.get("job_id") == job_id:
            hosts = item.get("hosts")
            return {
                "status": "queued",
                "job_id": job_id,
                "queue_position": idx,
                "queue": item.get("queue"),
                "hosts_filter": ",".join(hosts) if hosts else "",
                "cmd": str(item.get("cmd") or ""),
            }
    running_jobs = load_running_jobs()
    for host, raw_item in running_jobs.items():
        item = normalize_job_item(raw_item)
        if item.get("job_id") == job_id:
            started_at = raw_item.get("started_at") if isinstance(raw_item, dict) else None
            return {
                "status": "running",
                "job_id": job_id,
                "host": host,
                "remote_payload_path": item.get("payload_remote_path") or "",
                "started_at": _format_epoch(started_at),
                "queue": item.get("queue"),
                "cmd": str(item.get("cmd") or ""),
            }
    for record in reversed(load_completed_jobs()):
        if isinstance(record, dict) and record.get("job_id") == job_id:
            return {
                "status": "completed",
                "job_id": job_id,
                "host": record.get("host") or "",
                "remote_payload_path": record.get("payload_remote_path") or "",
                "started_at": _format_epoch(record.get("started_at")),
                "finished_at": _format_epoch(record.get("finished_at")),
                "duration": record.get("dur") or "",
                "queue": record.get("queue") or "",
                "cmd": str(record.get("cmd") or ""),
            }
    return None


def _query_job_state_remote(queue_host, job_id):
    remote_argv = ["awsqueueengine", "job-info", job_id]
    remote_cmd = shlex.join(remote_argv)
    result = subprocess.run([SSH_BIN, queue_host, remote_cmd], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"remote job-info failed (rc={result.returncode}): {detail}")
    raw = (result.stdout or "").strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"could not parse remote job-info output: {exc}; raw={raw!r}")
    if not parsed:
        return None
    return parsed


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

    job_id = getattr(args, "job_id", None) or new_job_tag()
    remote_argv = _build_remote_submit_argv(
        args,
        command,
        payload_s3_uri=payload_s3_uri,
        payload_size_bytes=payload_size_bytes,
        job_id=job_id,
    )
    result = _run_remote_submit(args.queue_host, remote_argv)
    if result.stdout:
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n", flush=True)
    if result.stderr:
        print(result.stderr, end="" if result.stderr.endswith("\n") else "\n", file=sys.stderr, flush=True)
    if result.returncode != 0:
        sys.exit(result.returncode)

    queue_name = getattr(args, "queue", None) or getattr(args, "host_set", None) or DEFAULT_QUEUE
    _write_local_run_info(
        args.payload,
        {
            "job_id": job_id,
            "submitted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "queue_host": args.queue_host,
            "queue": queue_name,
            "cmd": command,
            "payload_s3_uri": payload_s3_uri or "",
        },
    )
    print(f"Submitted {job_id}", flush=True)


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
    p_qstat = sub.add_parser("qstat", help="Show running jobs tracked by monitor")
    p_qstat.add_argument(
        "--queue-host",
        help="Show running jobs from a remote queue host over SSH",
        default=None,
    )
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
        help="Deprecated alias for --queue.",
        default=None,
    )
    p_submit.add_argument("--queue", help="Submit to a named user queue configured on the queue host", default=None)
    p_submit.add_argument("--priority", type=int, default=None, help="Integer priority (higher runs first)")
    p_submit.add_argument("--high-priority", action="store_true", help="Mark this job as high priority in the queue")
    p_submit.add_argument("--preempt", action="store_true", help="Allow this job to preempt a running managed job if needed")
    p_submit.add_argument("--queue-host", help="Forward this submit to a remote queue host over SSH", default=None)
    p_submit.add_argument("--payload-s3-uri", default=None, help=argparse.SUPPRESS)
    p_submit.add_argument("--payload-size-bytes", type=int, default=None, help=argparse.SUPPRESS)
    p_submit.add_argument("--job-id", default=None, help=argparse.SUPPRESS)
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

    p_list = sub.add_parser("list", help="Show queued jobs")
    p_list.add_argument(
        "--queue-host",
        help="Show queued jobs from a remote queue host over SSH",
        default=None,
    )
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

    p_info = sub.add_parser(
        "info",
        help="Refresh local run.info from queue host state (host, remote payload, status)",
    )
    p_info.add_argument(
        "--payload",
        "-p",
        help="Local payload directory containing run.info (default: current directory)",
        default=None,
    )
    p_info.add_argument(
        "--queue-host",
        help="Queue host to query (default: queue_host value from run.info)",
        default=None,
    )

    p_job_info = sub.add_parser("job-info", help="Emit JSON state for a job_id from local queue/running/completed")
    p_job_info.add_argument("job_id")

    args = parser.parse_args()

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
        if args.host_set:
            queue_name = normalize_queue_name(args.host_set)
            queue_host_map = _resolve_queue_hosts_for_cli(args.hosts_file)
            monitor_hosts = queue_host_map.get(queue_name) or _resolve_host_set(args.host_set)
        else:
            queue_host_map = _resolve_queue_hosts_for_cli(args.hosts_file)
            monitor_hosts = list(dict.fromkeys(host for hosts in queue_host_map.values() for host in hosts))
        rows = status_all(monitor_hosts)
        print(f"{'HOST':8}  {'REACH':8}  {'PID':8}  {'TAG':12}  INFO", flush=True)
        for r in rows:
            reach = "yes" if r["reachable"] else "no"
            pid = r["pid"] or "-"
            tag = r["tag"] or "-"
            info = (r["raw"][:60] + "...") if r["raw"] else ""
            print(f"{r['host']:8}  {reach:8}  {pid:8}  {tag:12}  {info}", flush=True)
    elif args.cmd == "qstat":
        if getattr(args, "queue_host", None):
            _proxy_remote_cli(args.queue_host, ["awsqueueengine", "qstat"])
            return
        running_jobs = load_running_jobs()
        if not running_jobs:
            print("(no running jobs tracked)", flush=True)
        else:
            print(
                f"{'HOST':8}  {'JOB':22}  {'DUR':8}  {'PRI':5}  {'PREEMPT':7}  {'QUEUE':12}  "
                f"{'HOSTS':15}  {'PAYLOAD':24}  CMD",
                flush=True,
            )
            for host in sorted(running_jobs):
                raw_item = running_jobs[host]
                item = normalize_job_item(raw_item)
                hosts_text = ",".join(item["hosts"]) if item["hosts"] else "any"
                payload_text = _payload_display_text(item)
                cmd_text = str(item.get("cmd") or "")
                dur_text = _format_elapsed(raw_item.get("started_at") if isinstance(raw_item, dict) else None)
                job_id_text = item.get("job_id") or "-"
                print(
                    f"{host:8}  {job_id_text:22}  {dur_text:8}  {item['priority']:5d}  {str(item['preempt']):7}  "
                    f"{item['queue'][:12]:12}  {hosts_text[:15]:15}  {payload_text:24}  "
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
        if args.queue and args.host_set:
            print("--queue and --host-set cannot be used together.", flush=True)
            sys.exit(1)
        if args.queue and args.hosts:
            print("--queue and --hosts cannot be used together.", flush=True)
            sys.exit(1)

        command = " ".join(args.command).strip()
        if not command:
            print("No command provided.", flush=True)
            sys.exit(1)

        if args.payload:
            payload_path = Path(args.payload).expanduser()
            if not payload_path.exists():
                print(f"Payload not found on local filesystem: {payload_path}", flush=True)
                sys.exit(1)

        if args.queue_host:
            _handle_remote_submit(args, command)
            return

        if args.host_set and args.hosts_file:
            print("--host-set and --hosts-file cannot be used together.", flush=True)
            sys.exit(1)
        if args.queue and args.hosts_file:
            print("--queue and --hosts-file cannot be used together; configure queues on the queue host.", flush=True)
            sys.exit(1)

        queue_name = normalize_queue_name(args.queue or args.host_set or DEFAULT_QUEUE)
        queue_host_map = _resolve_queue_hosts_for_cli(args.hosts_file)
        if queue_name not in queue_host_map:
            valid_queues = ", ".join(sorted(queue_host_map)) if queue_host_map else "(none)"
            print(f"Unknown queue {queue_name!r}. Valid queues: {valid_queues}", flush=True)
            sys.exit(1)

        hosts = None
        valid_hosts = set(queue_host_map.get(queue_name, []))
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

        job_id = args.job_id or new_job_tag()
        local_payload_path = None if args.payload_s3_uri else args.payload
        item = {
            "cmd": command,
            "payload": local_payload_path,
            "priority": priority,
            "queue": queue_name,
            "hosts": hosts,
            "preempt": args.preempt,
            "payload_s3_uri": args.payload_s3_uri,
            "payload_size_bytes": args.payload_size_bytes,
            "job_id": job_id,
        }
        enqueue_item(item)
        print("Enqueued:", item, flush=True)
        # Only write run.info / print Submitted on the original submitter's machine.
        # When this CLI invocation is a forwarded remote submit (--job-id was passed in),
        # the local-side caller will print Submitted and write run.info.
        if not args.job_id:
            _write_local_run_info(
                local_payload_path,
                {
                    "job_id": job_id,
                    "submitted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "queue_host": "local",
                    "queue": queue_name,
                    "cmd": command,
                    "payload_s3_uri": args.payload_s3_uri or "",
                },
            )
            print(f"Submitted {job_id}", flush=True)
    elif args.cmd == "requeue-running":
        running_jobs = load_running_jobs()
        queue_host_map = _resolve_queue_hosts_for_cli(args.hosts_file)
        valid_hosts = {host for hosts in queue_host_map.values() for host in hosts}
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
        if getattr(args, "queue_host", None):
            _proxy_remote_cli(args.queue_host, ["awsqueueengine", "list"])
            return
        q = load_queue()
        if not q:
            print("(queue empty)", flush=True)
        else:
            for i, raw_item in enumerate(q, 1):
                item = normalize_job_item(raw_item)
                hosts_text = ",".join(item["hosts"]) if item["hosts"] else "-"
                payload_text = _payload_display_text(item)
                job_id_text = item.get("job_id") or "-"
                print(
                    f"{i:3d}. [job={job_id_text}] [priority={item['priority']}] [queue={item['queue']}] "
                    f"[hosts={hosts_text}] [preempt={item['preempt']}] "
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
            hosts_text = ",".join(item["hosts"]) if item["hosts"] else "-"
            payload_text = _payload_display_text(item)
            print(
                f"  {idx:3d}. [priority={item['priority']}] [queue={item['queue']}] [hosts={hosts_text}] [preempt={item['preempt']}] "
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

        source_kind, _source_value = get_configured_queue_source()
        monitor_hosts = HOSTS if source_kind else _resolve_hosts_for_cli(args.hosts_file)
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
    elif args.cmd == "job-info":
        state = lookup_job_state(args.job_id)
        print(json.dumps(state if state is not None else {}), flush=True)
    elif args.cmd == "info":
        payload_dir = Path(args.payload).expanduser() if args.payload else Path.cwd()
        info_path = payload_dir / "run.info"
        if not info_path.exists():
            print(f"run.info not found at {info_path}", flush=True)
            sys.exit(1)
        existing = _read_run_info_file(info_path)
        job_id = existing.get("job_id")
        if not job_id:
            print(f"No job_id in {info_path}", flush=True)
            sys.exit(1)
        queue_host = args.queue_host or existing.get("queue_host") or "local"
        if queue_host == "local":
            state = lookup_job_state(job_id)
        else:
            try:
                state = _query_job_state_remote(queue_host, job_id)
            except RuntimeError as exc:
                print(f"Failed to query {queue_host}: {exc}", flush=True)
                sys.exit(1)
        if not state:
            print(f"Job {job_id} not found in queue host state (queued/running/completed).", flush=True)
            sys.exit(1)
        merged = dict(existing)
        for key, value in state.items():
            if value is None or value == "":
                continue
            merged[key] = value
        _write_run_info_file(info_path, merged)
        print(
            f"Updated {info_path}: status={merged.get('status', '?')} "
            f"host={merged.get('host', '-')} remote_payload={merged.get('remote_payload_path', '-')}",
            flush=True,
        )
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
