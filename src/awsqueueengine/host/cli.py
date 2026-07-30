"""Host-side CLI: `awsqe-host`.

Operates on the queue host's local state files and the monitor daemon.
Phase 4 replaces start-monitor/stop-monitor/status-monitor with systemd-style
install/start/stop/status verbs.
"""
import argparse
import json
import os
import signal
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from ..shared.cli_utils import (
    QDEL_HELP,
    add_qdel_arguments,
    join_command_argv,
    qdel_selectors,
    validate_qdel_selectors,
)
from ..shared.config import HOSTS, HOSTS_FILE
from ..shared.deferred_state import load_deferred_jobs, pop_all_deferred, pop_deferred_by_indices
from ..shared.failure_state import load_failed_jobs
from ..shared.job_lookup import enrich_selection_message, lookup_job_state
from ..shared.paths import PIDFILE
from ..shared.queue import (
    QueueSelectionError,
    build_resume_item,
    enqueue_item,
    load_queue,
    normalize_job_item,
    remove_queue_positions,
    resolve_queue_selection,
    save_queue,
)
from ..shared.queue_config import (
    DEFAULT_QUEUE,
    QueueConfigSource,
    get_configured_queue_source,
    normalize_queue_name,
)
from ..shared.run_info import format_epoch
from ..shared.running_state import load_running_jobs
from ..shared.worker_actions import kill_managed_on_host, new_job_tag
from .monitor import (
    acquire_monitor_lock,
    clear_host_cooldowns,
    get_host_cooldowns,
    load_hosts_from_file,
    monitor_loop,
    release_monitor_lock,
)
from .notifications import parse_email_recipients, send_email


stop_event = threading.Event()


# ---------- pidfile + daemon helpers ----------

def write_pidfile():
    PIDFILE.parent.mkdir(parents=True, exist_ok=True)
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


def pid_is_running(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _handle_stop(signum, frame):
    print("Stopping monitor loop...")
    stop_event.set()


# ---------- formatting helpers ----------

def _format_elapsed(started_at):
    if not isinstance(started_at, (int, float)):
        return "?"
    elapsed_seconds = max(0, int(time.time() - float(started_at)))
    hours, rem = divmod(elapsed_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _payload_display_text(item):
    return item.get("payload_remote_path") or item.get("payload_s3_uri") or item.get("payload") or "-"


# ---------- hosts/queue resolution helpers ----------

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


# ---------- subcommand handlers ----------

def cmd_submit_local(args, command):
    """Local enqueue. Called by `awsqe-host submit` and by the legacy CLI when no --queue-host."""
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
        "mps": bool(getattr(args, "mps", False)),
        "payload_s3_uri": args.payload_s3_uri,
        "payload_size_bytes": args.payload_size_bytes,
        "job_id": job_id,
    }
    enqueue_item(item)
    print("Enqueued:", item, flush=True)
    # When this CLI invocation is a forwarded remote submit (--job-id was passed in),
    # the local-side caller will print Submitted and write run.info.
    if not args.job_id:
        from ..shared.run_info import write_local_run_info
        write_local_run_info(
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


def cmd_list(args):
    q = load_queue()
    if not q:
        print("(queue empty)", flush=True)
        return
    for i, raw_item in enumerate(q, 1):
        item = normalize_job_item(raw_item)
        hosts_text = ",".join(item["hosts"]) if item["hosts"] else "-"
        payload_text = _payload_display_text(item)
        job_id_text = item.get("job_id") or "-"
        mps_text = "[mps=True] " if item.get("mps") else ""
        print(
            f"{i:3d}. [job={job_id_text}] [priority={item['priority']}] [queue={item['queue']}] "
            f"[hosts={hosts_text}] [preempt={item['preempt']}] {mps_text}"
            f"cmd={item['cmd']!r} payload={payload_text!r}",
            flush=True
        )


def cmd_qstat(args):
    running_jobs = load_running_jobs()
    if not running_jobs:
        print("(no running jobs tracked)", flush=True)
        return
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


def cmd_qdel(args):
    selector_error = validate_qdel_selectors(args)
    if selector_error:
        print(selector_error, flush=True)
        sys.exit(1)

    job_ids, indices, queue = qdel_selectors(args)
    q = load_queue()
    try:
        selection = resolve_queue_selection(q, job_ids=job_ids, indices=indices, queue=queue)
    except QueueSelectionError as exc:
        print(enrich_selection_message(exc.message, exc.tokens), flush=True)
        sys.exit(1)

    removed_jobs = remove_queue_positions(q, selection)

    print(f"Removed {len(removed_jobs)} job(s).", flush=True)
    for idx, item, _token in removed_jobs:
        hosts_text = ",".join(item["hosts"]) if item["hosts"] else "-"
        payload_text = _payload_display_text(item)
        job_id_text = item.get("job_id") or "-"
        print(
            f"  {idx:3d}. [job={job_id_text}] [priority={item['priority']}] [queue={item['queue']}] "
            f"[hosts={hosts_text}] [preempt={item['preempt']}] "
            f"cmd={item['cmd']!r} payload={payload_text!r}",
            flush=True,
        )


def cmd_clear(args):
    save_queue([])
    print("Queue cleared.", flush=True)


def cmd_deferred(args):
    jobs = load_deferred_jobs()
    if not jobs:
        print("(no deferred jobs)", flush=True)
        return
    for i, raw_item in enumerate(jobs, 1):
        item = normalize_job_item(raw_item)
        hosts_text = ",".join(item["hosts"]) if item["hosts"] else "-"
        payload_text = _payload_display_text(item)
        job_id_text = item.get("job_id") or "-"
        deferred_at = raw_item.get("deferred_at") if isinstance(raw_item, dict) else None
        last_host = raw_item.get("last_host") if isinstance(raw_item, dict) else None
        last_error_raw = raw_item.get("last_error") if isinstance(raw_item, dict) else None
        last_error = (last_error_raw or "-")
        if len(last_error) > 120:
            last_error = last_error[:117] + "..."
        deferred_at_text = format_epoch(deferred_at) or "-"
        print(
            f"{i:3d}. [job={job_id_text}] [priority={item['priority']}] [queue={item['queue']}] "
            f"[hosts={hosts_text}] [last_host={last_host or '-'}] [deferred_at={deferred_at_text}] "
            f"cmd={item['cmd']!r} payload={payload_text!r} last_error={last_error!r}",
            flush=True,
        )


def cmd_failed(args):
    records = [r for r in load_failed_jobs() if isinstance(r, dict)]
    if args.job_id:
        records = [r for r in records if r.get("job_id") == args.job_id]
    if not records:
        print("(no failed jobs recorded)", flush=True)
        return
    limit = max(1, int(args.limit or 50))
    for record in list(reversed(records))[:limit]:
        exit_code = record.get("exit_code")
        exit_text = "-" if exit_code is None else str(exit_code)
        failed_at = format_epoch(record.get("failed_at") or record.get("finished_at")) or "-"
        payload_text = _payload_display_text(normalize_job_item(record))
        print(
            f"[{failed_at}] [job={record.get('job_id') or '-'}] [host={record.get('host') or '-'}] "
            f"[queue={record.get('queue') or 'default'}] [dur={record.get('dur') or '-'}] "
            f"[exit={exit_text}] [reason={record.get('failure_reason') or 'unknown'}] "
            f"cmd={str(record.get('cmd') or '')!r} payload={payload_text!r}",
            flush=True,
        )
        if record.get("failure_detail"):
            print(f"    -> {record['failure_detail']}", flush=True)
        if args.log and record.get("log_tail"):
            for line in str(record["log_tail"]).splitlines():
                print(f"    |  {line}", flush=True)


def cmd_requeue_deferred(args):
    if args.all and args.indices:
        print("--all cannot be combined with explicit indices.", flush=True)
        sys.exit(1)
    if not args.all and not args.indices:
        print("Provide deferred index(es) or pass --all.", flush=True)
        sys.exit(1)

    jobs = load_deferred_jobs()
    if not jobs:
        print("(no deferred jobs)", flush=True)
        return

    if args.all:
        popped_pairs = pop_all_deferred()
    else:
        invalid = [i for i in args.indices if i < 1 or i > len(jobs)]
        if invalid:
            print(
                f"Invalid deferred index(es): {', '.join(str(i) for i in sorted(set(invalid)))}. "
                f"Deferred size: {len(jobs)}",
                flush=True,
            )
            sys.exit(1)
        popped_pairs = pop_deferred_by_indices(args.indices)

    if not popped_pairs:
        print("No deferred jobs were moved.", flush=True)
        return

    action_label = "Dropped" if args.drop else "Requeued"
    for idx, raw_item in popped_pairs:
        item = normalize_job_item(raw_item)
        item["submit_failures"] = 0
        if not args.drop:
            enqueue_item(item)
        hosts_text = ",".join(item["hosts"]) if item["hosts"] else "-"
        payload_text = _payload_display_text(item)
        print(
            f"  {idx:3d}. [job={item.get('job_id') or '-'}] [priority={item['priority']}] "
            f"[queue={item['queue']}] [hosts={hosts_text}] "
            f"cmd={item['cmd']!r} payload={payload_text!r}",
            flush=True,
        )
    print(f"{action_label} {len(popped_pairs)} deferred job(s).", flush=True)


def cmd_requeue_running(args):
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

    # --mps forces the wrapper on; without it each job keeps its current setting.
    mps_override = True if getattr(args, "mps", False) else None

    requeued_count = 0
    for host in target_hosts:
        running_item = running_jobs.get(host)
        if not running_item:
            print(f"No tracked running job on {host}; skipping requeue.", flush=True)
            continue

        resume_item = build_resume_item(running_item, host, priority=100, mps=mps_override)
        q = load_queue()
        q.insert(0, resume_item)
        save_queue(q)
        requeued_count += 1
        mps_note = " with MPS enabled" if resume_item.get("mps") else ""
        print(
            f"Requeued running job for {host} at priority 100{mps_note}: "
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


def cmd_enable_host(args):
    if args.all and args.hosts:
        print("--all cannot be combined with explicit host names.", flush=True)
        sys.exit(1)

    if not args.all and not args.hosts:
        cooldowns = get_host_cooldowns()
        if not cooldowns:
            print("(no host cooldowns active)", flush=True)
            return
        print(f"{'HOST':12}  {'UNTIL':20}  REMAINING", flush=True)
        now_ts = time.time()
        for host in sorted(cooldowns):
            until_ts = cooldowns[host]
            until_text = format_epoch(until_ts) or "-"
            remaining_seconds = max(0, int(until_ts - now_ts))
            hours, rem = divmod(remaining_seconds, 3600)
            minutes, seconds = divmod(rem, 60)
            remaining_text = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            print(f"{host:12}  {until_text:20}  {remaining_text}", flush=True)
        return

    cleared = clear_host_cooldowns(hosts=args.hosts or None, all_hosts=args.all)
    if not cleared:
        if args.all:
            print("(no host cooldowns active)", flush=True)
        else:
            requested = ", ".join(args.hosts)
            print(f"No active cooldown for: {requested}", flush=True)
        return
    print(f"Released {len(cleared)} host(s) from cooldown: {', '.join(cleared)}", flush=True)


def cmd_job_info(args):
    state = lookup_job_state(args.job_id)
    print(json.dumps(state if state is not None else {}), flush=True)


def cmd_monitor(args):
    """Foreground monitor runner. Called by `awsqe-host monitor` (and by
    the systemd unit's ExecStart), and used by the legacy
    `awsqueueengine start-monitor` shim for backward compat."""
    # First-restart-after-upgrade: pick up any legacy ~/.aws_slurm_like_*.json
    # before the monitor loop starts reading state. Idempotent after that.
    from . import migration
    auto_result = migration.auto_migrate_if_needed()
    if auto_result and auto_result.moved:
        print(
            f"[migration] moved {len(auto_result.moved)} legacy state file(s) "
            f"into {auto_result.moved[0][1].parent}",
            flush=True,
        )
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


def cmd_stop_monitor(args) -> int:
    """Send SIGTERM to the pidfile-tracked monitor. Returns 0 on success, 1 on
    'nothing to stop'. Returns int instead of sys.exiting so callers (the
    daemon module's fallback, the legacy shim) can route the exit code themselves."""
    pid = read_pidfile()
    if not pid:
        print("Monitor not running (no pidfile).", flush=True)
        return 1

    if not pid_is_running(pid):
        print(f"Stale pidfile found (pid={pid}); cleaning up.", flush=True)
        remove_pidfile()
        return 1

    print(f"Stopping monitor (pid={pid})...", flush=True)
    os.kill(pid, signal.SIGTERM)
    return 0


def cmd_status_monitor(args) -> int:
    """Report monitor running/not-running. Returns 0 if running, 1 otherwise,
    so the exit code is scriptable from CI / health checks."""
    pid = read_pidfile()
    if not pid:
        print("Monitor not running.", flush=True)
        return 1

    if pid_is_running(pid):
        print(f"Monitor running (pid={pid})", flush=True)
        return 0
    print(f"Monitor NOT running (stale pidfile pid={pid})", flush=True)
    return 1


def cmd_test_email():
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


# ---------- argparse wiring ----------

def _add_submit_subparser(sub):
    p = sub.add_parser("submit", help="Enqueue a job locally on this queue host")
    p.add_argument("--hosts-file", default=None)
    p.add_argument("--payload", "-p", default=None)
    p.add_argument("--hosts", action="append", default=None)
    p.add_argument("--host-set", default=None, help="Deprecated alias for --queue.")
    p.add_argument("--queue", default=None)
    p.add_argument("--priority", type=int, default=None)
    p.add_argument("--high-priority", action="store_true")
    p.add_argument("--preempt", action="store_true")
    p.add_argument("--mps", action="store_true", help="Wrap the command in the NVIDIA MPS launch/teardown script.")
    p.add_argument("--payload-s3-uri", default=None)
    p.add_argument("--payload-size-bytes", type=int, default=None)
    p.add_argument("--job-id", default=None)
    p.add_argument("command", nargs=argparse.REMAINDER)
    return p


def _add_requeue_running_subparser(sub):
    p = sub.add_parser("requeue-running", help="Kill running managed job(s) and requeue at priority 100")
    p.add_argument("--hosts-file", default=None)
    p.add_argument(
        "--mps",
        action="store_true",
        help="Force the NVIDIA MPS wrapper on the requeued job(s) (default: keep each job's current setting).",
    )
    target = p.add_mutually_exclusive_group(required=True)
    target.add_argument("--hosts", action="append", default=None)
    target.add_argument("--all", "-all", action="store_true")
    return p


def _add_qdel_subparser(sub):
    p = sub.add_parser("qdel", help=QDEL_HELP)
    add_qdel_arguments(p)
    return p


def _add_requeue_deferred_subparser(sub):
    p = sub.add_parser("requeue-deferred", help="Move deferred job(s) back into the main queue")
    p.add_argument("indices", nargs="*", type=int, default=[])
    p.add_argument("--all", "-all", action="store_true")
    p.add_argument("--drop", action="store_true")
    return p


def _add_enable_host_subparser(sub):
    p = sub.add_parser("enable-host", help="Show active host cooldowns; with HOST(s) or --all, release them early")
    p.add_argument("hosts", nargs="*")
    p.add_argument("--all", "-all", action="store_true")
    return p


def _add_monitor_subparser(sub):
    p = sub.add_parser(
        "monitor",
        help="Run the monitor loop in the foreground (used as systemd ExecStart).",
    )
    p.add_argument("--hosts-file", default=None)
    return p


def _add_daemon_subparser(sub, name, help_text):
    p = sub.add_parser(name, help=help_text)
    p.add_argument("--user", action="store_true", help="Operate on the per-user unit instead of the system unit.")
    p.add_argument("--dry-run", action="store_true", help="Print what would happen; don't run anything.")
    return p


def build_parser():
    parser = argparse.ArgumentParser(prog="awsqe-host", description="AWSQueueEngine queue-host CLI")
    parser.add_argument("--test-email-connection", action="store_true", help="Send a test alert email and exit.")
    sub = parser.add_subparsers(dest="cmd")

    _add_submit_subparser(sub)
    sub.add_parser("list", help="Show queued jobs")
    sub.add_parser("qstat", help="Show running jobs tracked by monitor")
    _add_qdel_subparser(sub)
    sub.add_parser("clear", help="Clear the queue")
    sub.add_parser("deferred", help="Show deferred jobs")
    p_failed = sub.add_parser("failed", help="Show jobs that failed, newest first")
    p_failed.add_argument("--limit", "-n", type=int, default=50, help="How many recent failures to show (default 50)")
    p_failed.add_argument("--job-id", default=None, help="Only show failures for this job id")
    p_failed.add_argument("--log", action="store_true", help="Also print the captured tail of each job log")
    _add_requeue_deferred_subparser(sub)
    _add_requeue_running_subparser(sub)
    _add_enable_host_subparser(sub)
    p_job_info = sub.add_parser("job-info", help="Emit JSON state for a job_id")
    p_job_info.add_argument("job_id")
    _add_monitor_subparser(sub)

    p_migrate = sub.add_parser(
        "migrate",
        help="One-shot move of legacy ~/.aws_slurm_like_*.json state into ~/.awsqe/host/.",
    )
    p_migrate.add_argument("--dry-run", action="store_true", help="Print what would happen; touch nothing.")
    p_migrate.add_argument("--force", action="store_true", help="Re-run migration even if it already completed.")

    # systemd-style daemon verbs
    p_install = _add_daemon_subparser(sub, "install", "Install the systemd unit and enable --now.")
    p_install.add_argument("--force", action="store_true", help="Overwrite an existing unit file.")
    _add_daemon_subparser(sub, "uninstall", "Disable + remove the systemd unit.")
    _add_daemon_subparser(sub, "start", "Start the daemon (systemctl start; foreground fallback).")
    _add_daemon_subparser(sub, "stop", "Stop the daemon (systemctl stop; pidfile fallback).")
    _add_daemon_subparser(sub, "restart", "Restart the daemon (systemctl restart).")
    _add_daemon_subparser(sub, "status", "Show daemon status (systemctl status; pidfile fallback).")
    p_logs = _add_daemon_subparser(sub, "logs", "Tail journal logs for the daemon.")
    p_logs.add_argument("-f", "--follow", action="store_true", help="Follow new entries.")
    p_logs.add_argument("-n", "--lines", type=int, default=None, help="Show the last N lines.")

    sub.add_parser("rpc", help="Read one JSON RPC request from stdin and write a response to stdout")

    return parser


def dispatch(args, parser=None):
    if getattr(args, "test_email_connection", False):
        cmd_test_email()
        return

    cmd = getattr(args, "cmd", None)
    if cmd == "submit":
        if not args.command:
            print("No command provided.", flush=True)
            sys.exit(1)
        command = join_command_argv(args.command)
        if not command:
            print("No command provided.", flush=True)
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
        if args.payload:
            payload_path = Path(args.payload).expanduser()
            if not payload_path.exists():
                print(f"Payload not found on local filesystem: {payload_path}", flush=True)
                sys.exit(1)
        cmd_submit_local(args, command)
    elif cmd == "list":
        cmd_list(args)
    elif cmd == "qstat":
        cmd_qstat(args)
    elif cmd == "qdel":
        cmd_qdel(args)
    elif cmd == "clear":
        cmd_clear(args)
    elif cmd == "deferred":
        cmd_deferred(args)
    elif cmd == "failed":
        cmd_failed(args)
    elif cmd == "requeue-deferred":
        cmd_requeue_deferred(args)
    elif cmd == "requeue-running":
        cmd_requeue_running(args)
    elif cmd == "enable-host":
        cmd_enable_host(args)
    elif cmd == "job-info":
        cmd_job_info(args)
    elif cmd == "monitor":
        cmd_monitor(args)
    elif cmd == "migrate":
        from . import migration
        result = migration.migrate(dry_run=bool(args.dry_run), force=bool(args.force))
        print(migration.render_summary(result, dry_run=bool(args.dry_run)), flush=True)
        sys.exit(0)
    elif cmd in {"install", "uninstall", "start", "stop", "restart", "status", "logs"}:
        from . import daemon as daemon_mod
        user_mode = bool(getattr(args, "user", False))
        dry_run = bool(getattr(args, "dry_run", False))
        if cmd == "install":
            sys.exit(daemon_mod.install(user_mode=user_mode, force=bool(args.force), dry_run=dry_run))
        if cmd == "uninstall":
            sys.exit(daemon_mod.uninstall(user_mode=user_mode, dry_run=dry_run))
        if cmd == "start":
            sys.exit(daemon_mod.start(user_mode=user_mode, dry_run=dry_run))
        if cmd == "stop":
            sys.exit(daemon_mod.stop(user_mode=user_mode, dry_run=dry_run))
        if cmd == "restart":
            sys.exit(daemon_mod.restart(user_mode=user_mode, dry_run=dry_run))
        if cmd == "status":
            sys.exit(daemon_mod.status(user_mode=user_mode, dry_run=dry_run))
        if cmd == "logs":
            sys.exit(daemon_mod.logs(
                user_mode=user_mode,
                follow=bool(args.follow),
                lines=args.lines,
                dry_run=dry_run,
            ))
    elif cmd == "rpc":
        from . import rpc as rpc_module
        sys.exit(rpc_module.run_rpc_stdin_stdout())
    else:
        if parser is not None:
            parser.print_help()


def main():
    sys.stdout = open(sys.stdout.fileno(), mode='w', buffering=1, encoding=sys.stdout.encoding, closefd=False)
    sys.stderr = open(sys.stderr.fileno(), mode='w', buffering=1, encoding=sys.stderr.encoding, closefd=False)
    parser = build_parser()
    args = parser.parse_args()
    dispatch(args, parser=parser)


if __name__ == "__main__":
    main()
