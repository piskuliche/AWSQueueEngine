"""Client-side CLI: `awsqe-client`.

Runs on a user's machine. Drives the queue host over SSH and manages
the local payload archive + run.info bookkeeping. Phase 3 adds a
persisted `queue_host` setting at `~/.awsqe/client/config.toml`; until
then, every command that needs a queue host requires `--queue-host`.
"""
import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

from ..shared.config import HOSTS, HOSTS_FILE
from ..shared.host_status import status_all
from ..shared.job_lookup import lookup_job_state
from ..shared.queue_config import (
    DEFAULT_QUEUE,
    QueueConfigSource,
    get_configured_queue_source,
    load_hosts_from_file,
    normalize_queue_name,
)
from ..shared.run_info import (
    format_epoch,
    read_run_info_file,
    write_local_run_info,
    write_run_info_file,
)
from ..shared.worker_actions import kill_managed_on_host, new_job_tag, tail_remote_log
from .remote_query import proxy_remote_cli, query_job_state_remote
from .staging import sizeof_local_path_bytes, where_is_next_submit
from .submit import (
    archive_payload_to_temp,
    build_remote_submit_argv,
    run_remote_submit,
    upload_payload_archive_to_s3,
)


# ---------- helpers shared with the legacy CLI ----------

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
    """Look up a legacy host-set name from env (used by `status --host-set`)."""
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


# ---------- subcommand handlers ----------

def cmd_status(args):
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


def cmd_submit_remote(args, command):
    """Client-side remote submit: archive + S3 + SSH-invoke awsqe-host submit."""
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
            archive_path = archive_payload_to_temp(payload_path)
            payload_s3_uri = upload_payload_archive_to_s3(archive_path, payload_path.name)
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
    remote_argv = build_remote_submit_argv(
        args,
        command,
        payload_s3_uri=payload_s3_uri,
        payload_size_bytes=payload_size_bytes,
        job_id=job_id,
    )
    result = run_remote_submit(args.queue_host, remote_argv)
    if result.stdout:
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n", flush=True)
    if result.stderr:
        print(result.stderr, end="" if result.stderr.endswith("\n") else "\n", file=sys.stderr, flush=True)
    if result.returncode != 0:
        sys.exit(result.returncode)

    queue_name = getattr(args, "queue", None) or getattr(args, "host_set", None) or DEFAULT_QUEUE
    write_local_run_info(
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


def cmd_tail(args):
    r = tail_remote_log(args.host)
    if not r["ok"]:
        print("Error:", r.get("reason") or r.get("err"), flush=True)
    else:
        header = f"Host: {r['host']}  tag: {r.get('tag') or '(none)'}"
        print(header, flush=True)
        print("-" * len(header), flush=True)
        print(r.get("out") or "(no log output)", flush=True)


def cmd_stop(args):
    res = kill_managed_on_host(args.host)
    if res["rc"] == 0:
        print(f"Sent kill to managed job(s) on {args.host}.", flush=True)
    else:
        detail = res.get("err") or res.get("out") or "(no stderr/stdout returned)"
        print(f"Kill error (rc={res['rc']}): {detail}", flush=True)


def cmd_where(args):
    where_is_next_submit()


def cmd_info(args):
    payload_dir = Path(args.payload).expanduser() if args.payload else Path.cwd()
    info_path = payload_dir / "run.info"
    if not info_path.exists():
        print(f"run.info not found at {info_path}", flush=True)
        sys.exit(1)
    existing = read_run_info_file(info_path)
    job_id = existing.get("job_id")
    if not job_id:
        print(f"No job_id in {info_path}", flush=True)
        sys.exit(1)
    queue_host = args.queue_host or existing.get("queue_host") or "local"
    if queue_host == "local":
        state = lookup_job_state(job_id)
    else:
        try:
            state = query_job_state_remote(queue_host, job_id)
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
    write_run_info_file(info_path, merged)
    print(
        f"Updated {info_path}: status={merged.get('status', '?')} "
        f"host={merged.get('host', '-')} remote_payload={merged.get('remote_payload_path', '-')}",
        flush=True,
    )


def cmd_list_remote(args):
    proxy_remote_cli(args.queue_host, ["awsqueueengine", "list"])


def cmd_qstat_remote(args):
    proxy_remote_cli(args.queue_host, ["awsqueueengine", "qstat"])


def cmd_deferred_remote(args):
    proxy_remote_cli(args.queue_host, ["awsqueueengine", "deferred"])


def cmd_requeue_deferred_remote(args):
    if args.all and args.indices:
        print("--all cannot be combined with explicit indices.", flush=True)
        sys.exit(1)
    if not args.all and not args.indices:
        print("Provide deferred index(es) or pass --all.", flush=True)
        sys.exit(1)
    remote_argv = ["awsqueueengine", "requeue-deferred"]
    if args.all:
        remote_argv.append("--all")
    else:
        remote_argv.extend(str(i) for i in args.indices)
    if args.drop:
        remote_argv.append("--drop")
    proxy_remote_cli(args.queue_host, remote_argv)


def cmd_enable_host_remote(args):
    if args.all and args.hosts:
        print("--all cannot be combined with explicit host names.", flush=True)
        sys.exit(1)
    remote_argv = ["awsqueueengine", "enable-host"]
    if args.all:
        remote_argv.append("--all")
    else:
        remote_argv.extend(args.hosts)
    proxy_remote_cli(args.queue_host, remote_argv)


# ---------- argparse wiring ----------

def _require_queue_host(args, command):
    if not args.queue_host:
        print(
            f"awsqe-client {command} requires --queue-host (Phase 3 will read it from "
            f"~/.awsqe/client/config.toml).",
            flush=True,
        )
        sys.exit(2)


def build_parser():
    parser = argparse.ArgumentParser(prog="awsqe-client", description="AWSQueueEngine client (submitter) CLI")
    sub = parser.add_subparsers(dest="cmd")

    p_status = sub.add_parser("status", help="Show status for all hosts")
    p_status.add_argument("--hosts-file", default=None)
    p_status.add_argument("--host-set", default=None)

    p_submit = sub.add_parser("submit", help="Archive a payload, upload to S3, and enqueue on the queue host")
    p_submit.add_argument("--payload", "-p", default=None)
    p_submit.add_argument("--hosts", action="append", default=None)
    p_submit.add_argument("--host-set", default=None)
    p_submit.add_argument("--queue", default=None)
    p_submit.add_argument("--priority", type=int, default=None)
    p_submit.add_argument("--high-priority", action="store_true")
    p_submit.add_argument("--preempt", action="store_true")
    p_submit.add_argument("--queue-host", default=None)
    p_submit.add_argument("--hosts-file", default=None)
    p_submit.add_argument("command", nargs=argparse.REMAINDER)

    p_tail = sub.add_parser("tail", help="Tail remote log on a worker host")
    p_tail.add_argument("host")

    p_stop = sub.add_parser("stop", help="Kill managed job(s) on a worker host")
    p_stop.add_argument("host")

    sub.add_parser("where", help="Show where the next job would be submitted (probes scratch)")

    p_info = sub.add_parser("info", help="Refresh local run.info from queue host state")
    p_info.add_argument("--payload", "-p", default=None)
    p_info.add_argument("--queue-host", default=None)

    p_list = sub.add_parser("list", help="Show queued jobs on the queue host")
    p_list.add_argument("--queue-host", default=None)

    p_qstat = sub.add_parser("qstat", help="Show running jobs on the queue host")
    p_qstat.add_argument("--queue-host", default=None)

    p_deferred = sub.add_parser("deferred", help="Show deferred jobs on the queue host")
    p_deferred.add_argument("--queue-host", default=None)

    p_requeue_deferred = sub.add_parser("requeue-deferred", help="Requeue deferred job(s) on the queue host")
    p_requeue_deferred.add_argument("indices", nargs="*", type=int, default=[])
    p_requeue_deferred.add_argument("--all", "-all", action="store_true")
    p_requeue_deferred.add_argument("--drop", action="store_true")
    p_requeue_deferred.add_argument("--queue-host", default=None)

    p_enable_host = sub.add_parser("enable-host", help="View or release host cooldowns on the queue host")
    p_enable_host.add_argument("hosts", nargs="*")
    p_enable_host.add_argument("--all", "-all", action="store_true")
    p_enable_host.add_argument("--queue-host", default=None)

    return parser


def dispatch(args, parser=None):
    cmd = getattr(args, "cmd", None)
    if cmd == "status":
        cmd_status(args)
    elif cmd == "submit":
        if not args.command:
            print("No command provided.", flush=True)
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
        _require_queue_host(args, "submit")
        cmd_submit_remote(args, command)
    elif cmd == "tail":
        cmd_tail(args)
    elif cmd == "stop":
        cmd_stop(args)
    elif cmd == "where":
        cmd_where(args)
    elif cmd == "info":
        cmd_info(args)
    elif cmd == "list":
        _require_queue_host(args, "list")
        cmd_list_remote(args)
    elif cmd == "qstat":
        _require_queue_host(args, "qstat")
        cmd_qstat_remote(args)
    elif cmd == "deferred":
        _require_queue_host(args, "deferred")
        cmd_deferred_remote(args)
    elif cmd == "requeue-deferred":
        _require_queue_host(args, "requeue-deferred")
        cmd_requeue_deferred_remote(args)
    elif cmd == "enable-host":
        _require_queue_host(args, "enable-host")
        cmd_enable_host_remote(args)
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
