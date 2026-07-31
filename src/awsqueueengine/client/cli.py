"""Client-side CLI: `awsqe-client`.

Runs on a user's machine. Drives the queue host through the JSON-over-SSH
RPC defined in :mod:`awsqueueengine.shared.protocol`. The queue host and
S3 settings come from ``~/.awsqe/client/config.toml`` (managed via
``awsqe-client config set ...``) when not provided on the command line.
Resolution precedence is **CLI flag > env var > config > error**.
"""
import argparse
import os
import sys
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
from ..shared.host_status import status_all
from ..shared.job_lookup import lookup_job_state
from ..shared.job_status import (
    ALL_STATUSES,
    DELETED,
    FAILED,
    QUEUED,
    expand_status_filter,
    is_terminal,
)
from ..shared.protocol import RpcError, RpcTransportError
from ..shared.queue_config import (
    DEFAULT_QUEUE,
    QueueConfigSource,
    get_configured_queue_source,
    load_hosts_from_file,
    normalize_queue_name,
)
from ..shared.rpc_client import call as rpc_call
from ..shared.run_info import (
    format_epoch,
    read_run_info_file,
    write_local_run_info,
    write_run_info_file,
)
from ..shared.timespec import parse_time_spec
from ..shared.worker_actions import kill_managed_on_host, new_job_tag, tail_remote_log
from . import config as client_config
from . import ledger
from .config import (
    CONFIG_PATH,
    KEY_SCHEMA,
    effective_queue_host,
    effective_s3_bucket,
    effective_s3_prefix,
    load_config,
    normalize_key,
    save_config,
    set_value,
    unset_value,
)
from .staging import sizeof_local_path_bytes, where_is_next_submit
from .submit import archive_payload_to_temp, upload_payload_archive_to_s3


# ---------- helpers ----------

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


def _parse_cli_host_values(host_values):
    hosts = []
    for host_value in host_values or []:
        if not host_value:
            continue
        hosts.extend(h.strip() for h in host_value.split(",") if h and h.strip())
    return list(dict.fromkeys(hosts))


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


def _payload_display_text(item):
    return item.get("payload_remote_path") or item.get("payload_s3_uri") or item.get("payload") or "-"


def _resolve_queue_host(args, command):
    """Resolve queue_host from CLI flag > config; exit with a clear pointer on miss."""
    host = effective_queue_host(getattr(args, "queue_host", None))
    if not host:
        print(
            f"awsqe-client {command} needs a queue host. Pass --queue-host <host> "
            f"or run `awsqe-client config set queue-host <host>`.",
            flush=True,
        )
        sys.exit(2)
    args.queue_host = host
    return host


def _rpc(args, method, params=None):
    """Call the queue host's RPC. Exits with a clear error on failure."""
    try:
        return rpc_call(args.queue_host, method, params or {})
    except RpcTransportError as exc:
        print(f"RPC transport error talking to {args.queue_host}: {exc.detail}", flush=True, file=sys.stderr)
        sys.exit(1)
    except RpcError as exc:
        print(f"RPC error from {args.queue_host}: {exc.code}: {exc.message}", flush=True, file=sys.stderr)
        sys.exit(1)


# ---------- rendering helpers (match legacy `awsqueueengine` text format) ----------

def _render_queue_jobs(jobs):
    if not jobs:
        print("(queue empty)", flush=True)
        return
    for i, item in enumerate(jobs, 1):
        hosts_text = ",".join(item.get("hosts") or []) if item.get("hosts") else "-"
        payload_text = _payload_display_text(item)
        job_id_text = item.get("job_id") or "-"
        mps_text = "[mps=True] " if item.get("mps") else ""
        print(
            f"{i:3d}. [job={job_id_text}] [priority={item.get('priority', 0)}] [queue={item.get('queue', 'default')}] "
            f"[hosts={hosts_text}] [preempt={item.get('preempt', False)}] {mps_text}"
            f"cmd={item.get('cmd')!r} payload={payload_text!r}",
            flush=True,
        )


def _format_elapsed(started_at):
    import time
    if not isinstance(started_at, (int, float)):
        return "?"
    elapsed_seconds = max(0, int(time.time() - float(started_at)))
    hours, rem = divmod(elapsed_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _render_running_jobs(running):
    if not running:
        print("(no running jobs tracked)", flush=True)
        return
    print(
        f"{'HOST':8}  {'JOB':22}  {'DUR':8}  {'PRI':5}  {'PREEMPT':7}  {'QUEUE':12}  "
        f"{'HOSTS':15}  {'PAYLOAD':24}  CMD",
        flush=True,
    )
    for host in sorted(running):
        item = running[host]
        hosts_text = ",".join(item.get("hosts") or []) if item.get("hosts") else "any"
        payload_text = _payload_display_text(item)
        cmd_text = str(item.get("cmd") or "")
        dur_text = _format_elapsed(item.get("started_at"))
        job_id_text = item.get("job_id") or "-"
        queue_text = (item.get("queue") or "default")[:12]
        print(
            f"{host:8}  {job_id_text:22}  {dur_text:8}  {item.get('priority', 0):5d}  {str(item.get('preempt', False)):7}  "
            f"{queue_text:12}  {hosts_text[:15]:15}  {payload_text:24}  {cmd_text}",
            flush=True,
        )


def _render_deferred_jobs(jobs):
    if not jobs:
        print("(no deferred jobs)", flush=True)
        return
    for i, item in enumerate(jobs, 1):
        hosts_text = ",".join(item.get("hosts") or []) if item.get("hosts") else "-"
        payload_text = _payload_display_text(item)
        job_id_text = item.get("job_id") or "-"
        deferred_at = item.get("deferred_at")
        last_host = item.get("last_host")
        last_error = item.get("last_error") or "-"
        if len(last_error) > 120:
            last_error = last_error[:117] + "..."
        deferred_at_text = format_epoch(deferred_at) or "-"
        print(
            f"{i:3d}. [job={job_id_text}] [priority={item.get('priority', 0)}] [queue={item.get('queue', 'default')}] "
            f"[hosts={hosts_text}] [last_host={last_host or '-'}] [deferred_at={deferred_at_text}] "
            f"cmd={item.get('cmd')!r} payload={payload_text!r} last_error={last_error!r}",
            flush=True,
        )


def _render_failed_jobs(jobs, show_log=False):
    if not jobs:
        print("(no failed jobs recorded)", flush=True)
        return
    print(
        f"{'FAILED AT':20}  {'JOB':22}  {'HOST':8}  {'DUR':8}  {'EXIT':5}  {'REASON':18}  CMD",
        flush=True,
    )
    for item in jobs:
        exit_code = item.get("exit_code")
        exit_text = "-" if exit_code is None else str(exit_code)
        failed_at = format_epoch(item.get("failed_at") or item.get("finished_at")) or "-"
        print(
            f"{failed_at:20}  {(item.get('job_id') or '-'):22}  {(item.get('host') or '-'):8}  "
            f"{(item.get('dur') or '-'):8}  {exit_text:5}  {(item.get('failure_reason') or '-')[:18]:18}  "
            f"{str(item.get('cmd') or '')}",
            flush=True,
        )
        detail = item.get("failure_detail")
        if detail:
            print(f"{'':20}  -> {detail}", flush=True)
        if show_log and item.get("log_tail"):
            for line in str(item["log_tail"]).splitlines():
                print(f"{'':20}  |  {line}", flush=True)


def _tracked_dur_text(record):
    """One column doing double duty: elapsed for finished jobs, position for queued.

    Running jobs get "-": the host reports ``started_at`` as preformatted text
    in *its* timezone, so the client can't subtract it from its own clock.
    """
    if record.get("status") == QUEUED:
        position = record.get("queue_position")
        return f"q#{position}" if position else "-"
    return record.get("duration") or "-"


def _render_tracked_jobs(records, total=None):
    if not records:
        # "did my filter match nothing" and "have I submitted anything" are
        # different questions; answer whichever one was asked.
        print("(no tracked jobs)" if not total else "(no tracked jobs match the filter)", flush=True)
        return
    print(
        f"{'SUBMITTED':20}  {'JOB':22}  {'STATUS':10}  {'HOST':8}  {'QUEUE':12}  {'DUR':8}  CMD",
        flush=True,
    )
    for record in records:
        job_id = record.get("job_id") or "-"
        print(
            f"{(format_epoch(record.get('submitted_at')) or '-'):20}  {job_id:22}  "
            f"{(record.get('status') or '-')[:10]:10}  {(record.get('host') or '-')[:8]:8}  "
            f"{(record.get('queue') or '-')[:12]:12}  {_tracked_dur_text(record)[:8]:8}  "
            f"{str(record.get('cmd') or '')}",
            flush=True,
        )
        if record.get("status") == FAILED:
            exit_text = record.get("exit_code") or "-"
            print(
                f"{'':20}  -> {record.get('failure_reason') or 'unknown'} exit={exit_text} "
                f"(see `awsqe-client failed --job-id {job_id} --log`)",
                flush=True,
            )
    if total is not None and total != len(records):
        print(f"{len(records)} of {total} tracked job(s).", flush=True)


# ---------- tracked-job refresh ----------

#: A host with a smaller batch cap than ours answers with the remainder in
#: `skipped`; re-ask until it stops, but never spin.
_MAX_BATCH_ROUNDS = 20


def _job_info_one_by_one(host, job_ids):
    """N x `job_info`, for a queue host predating `job_info_batch`.

    An application error for one job is that job's answer; a transport error
    means ssh itself is broken, so it propagates rather than retrying 200 more.
    """
    states = {}
    for job_id in job_ids:
        try:
            states[job_id] = rpc_call(host, "job_info", {"job_id": job_id}).get("state")
        except RpcError:
            states[job_id] = None
    return states


def _job_info_batch(host, job_ids):
    """Resolve `job_ids` against `host`, falling back on an older queue host."""
    states = {}
    pending = list(job_ids)
    for _ in range(_MAX_BATCH_ROUNDS):
        if not pending:
            break
        try:
            result = rpc_call(host, "job_info_batch", {"job_ids": pending})
        except RpcError as exc:
            if exc.code != "unknown_method":
                raise
            print(
                f"[WARN] {host} has no job_info_batch (older awsqe-host); falling back to "
                f"{len(pending)} individual lookup(s). Upgrade the host, or use --no-refresh.",
                file=sys.stderr,
                flush=True,
            )
            states.update(_job_info_one_by_one(host, pending))
            return states
        states.update(result.get("states") or {})
        pending = [job_id for job_id in (result.get("skipped") or []) if job_id not in states]
    return states


def refresh_tracked_jobs(records, *, override_host=None, default_queue_host=None):
    """Ask each job's queue host for its current state.

    Returns ``({job_id: state|None}, [warning, ...])``. Never exits: the point
    of the ledger is that it is local, so an unreachable queue host still has
    to leave the user with their list and a last-known status for every job.
    """
    by_host = {}
    for record in records:
        if is_terminal(record.get("status")):
            continue
        host = override_host or record.get("queue_host") or default_queue_host
        if host:
            by_host.setdefault(host, []).append(record["job_id"])

    states = {}
    warnings = []
    for host in sorted(by_host):
        job_ids = by_host[host]
        try:
            host_states = _job_info_batch(host, job_ids)
        except RpcTransportError as exc:
            warnings.append(f"could not reach {host}: {exc.detail} "
                            f"({len(job_ids)} job(s) not refreshed)")
            continue
        except RpcError as exc:
            warnings.append(f"{host} refused the request: {exc.code}: {exc.message} "
                            f"({len(job_ids)} job(s) not refreshed)")
            continue
        if host_states and all(state is None for state in host_states.values()):
            # The host writes its state files with a plain write_text, so a read
            # landing mid-rewrite legitimately sees an empty queue. Flipping
            # every live job to `missing` on that evidence is worse than waiting.
            warnings.append(
                f"{host} has no record of any of its {len(job_ids)} tracked job(s); "
                f"treating that as a bad read rather than marking them all missing"
            )
            continue
        states.update(host_states)
    return states, warnings


# ---------- subcommand handlers ----------

def _status_hosts_from_map(args, queue_host_map):
    """Choose which hosts `status` probes from a queue->hosts map."""
    if args.host_set:
        queue_name = normalize_queue_name(args.host_set)
        return queue_host_map.get(queue_name) or _resolve_host_set(args.host_set)
    return list(dict.fromkeys(host for hosts in queue_host_map.values() for host in hosts))


def _fetch_remote_queue_host_map(args):
    """Ask the queue host for its queue->hosts map (the source of truth).

    Returns ``None`` when the queue host can't be reached, so the caller can
    fall back to local host resolution instead of failing outright.
    """
    try:
        result = rpc_call(args.queue_host, "stats", {})
    except (RpcError, RpcTransportError) as exc:
        detail = getattr(exc, "detail", None) or getattr(exc, "message", None) or str(exc)
        print(
            f"[WARN] Could not fetch host list from queue host {args.queue_host}: {detail}. "
            f"Falling back to local host config.",
            file=sys.stderr,
            flush=True,
        )
        return None
    raw_map = result.get("queue_host_map") or {}
    return {queue: list(hosts) for queue, hosts in raw_map.items() if hosts}


def cmd_status(args):
    if args.host_set and args.hosts_file:
        print("--host-set and --hosts-file cannot be used together.", flush=True)
        sys.exit(1)

    # The queue host is the source of truth for the host pool. A client machine
    # usually has no local queues.json, so resolving hosts locally falls back to
    # the built-in eci1..eci20 default and silently misses any host past 20. Pull
    # the map from the queue host (like `qstat` does) unless the user forced a
    # local source with --hosts-file.
    queue_host_map = None
    if not args.hosts_file:
        queue_host = effective_queue_host(getattr(args, "queue_host", None))
        if queue_host:
            args.queue_host = queue_host
            queue_host_map = _fetch_remote_queue_host_map(args)
    if queue_host_map is None:
        queue_host_map = _resolve_queue_hosts_for_cli(args.hosts_file)

    monitor_hosts = _status_hosts_from_map(args, queue_host_map)
    rows = status_all(monitor_hosts)
    print(f"{'HOST':8}  {'REACH':8}  {'PID':8}  {'TAG':12}  INFO", flush=True)
    for r in rows:
        reach = "yes" if r["reachable"] else "no"
        pid = r["pid"] or "-"
        tag = r["tag"] or "-"
        info = (r["raw"][:60] + "...") if r["raw"] else ""
        print(f"{r['host']:8}  {reach:8}  {pid:8}  {tag:12}  {info}", flush=True)


def cmd_submit_remote(args, command):
    """Client-side remote submit: archive + S3 + RPC enqueue."""
    if args.hosts_file:
        print("--hosts-file is not supported with --queue-host; host validation happens on the queue host.", flush=True)
        sys.exit(1)

    payload_s3_uri = None
    payload_size_bytes = None
    archive_path = None
    if args.payload:
        client_cfg = load_config()
        bucket = effective_s3_bucket(client_cfg)
        prefix = effective_s3_prefix(client_cfg)
        payload_path = Path(args.payload).expanduser()
        payload_size_bytes = sizeof_local_path_bytes(payload_path)
        try:
            archive_path = archive_payload_to_temp(payload_path)
            payload_s3_uri = upload_payload_archive_to_s3(
                archive_path, payload_path.name, bucket=bucket, prefix=prefix,
            )
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
    queue_name = getattr(args, "queue", None) or getattr(args, "host_set", None) or DEFAULT_QUEUE
    hosts_param = _parse_cli_host_values(getattr(args, "hosts", None)) or None
    params = {
        "cmd": command,
        "queue": queue_name,
        "job_id": job_id,
        "preempt": bool(getattr(args, "preempt", False)),
        "mps": bool(getattr(args, "mps", False)),
    }
    if hosts_param:
        params["hosts"] = hosts_param
    if args.priority is not None:
        params["priority"] = args.priority
    elif args.high_priority:
        params["high_priority"] = True
    if payload_s3_uri:
        params["payload_s3_uri"] = payload_s3_uri
    if payload_size_bytes is not None:
        params["payload_size_bytes"] = payload_size_bytes

    result = _rpc(args, "enqueue", params)
    # Trust the server's job_id if it differs (it shouldn't, since we provided one).
    job_id = result.get("job_id") or job_id

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
    # run.info only exists when there's a payload directory to put it in, so
    # this is the only record a payload-less submit leaves behind.
    ledger.record_submission(
        job_id=job_id,
        queue_host=args.queue_host,
        queue=queue_name,
        cmd=command,
        # Absolute: the row has to still mean something listed from another cwd.
        payload=str(Path(args.payload).expanduser().resolve()) if args.payload else "",
        payload_s3_uri=payload_s3_uri or "",
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


def _info_target_from_job_id(args, token):
    """Resolve `info --job-id` against the ledger: (job_id, info_path, existing, queue_host).

    ``run.info`` becomes optional here — the job is selected by id, and a
    payload directory is only somewhere to *write* the refreshed state if the
    job happens to have one that still exists.
    """
    try:
        record = ledger.find_record(token)
    except ledger.LedgerSelectionError as exc:
        print(exc.message, flush=True)
        sys.exit(1)
    if record is None:
        print(
            f"No tracked job matching {token!r}. Run `awsqe-client jobs` to see what "
            "this machine has submitted.",
            flush=True,
        )
        sys.exit(1)

    payload_text = args.payload or record.get("payload") or ""
    payload_dir = Path(payload_text).expanduser() if payload_text else None
    info_path = payload_dir / "run.info" if payload_dir and payload_dir.is_dir() else None
    existing = read_run_info_file(info_path) if info_path and info_path.exists() else {}
    queue_host = (
        args.queue_host or record.get("queue_host") or effective_queue_host(None) or "local"
    )
    return record["job_id"], info_path, existing, queue_host


def _info_target_from_payload(args):
    """The original payload-directory path: run.info is required and names the job."""
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
    # Precedence: CLI flag > run.info > config > "local" (do lookup locally).
    queue_host = (
        args.queue_host
        or existing.get("queue_host")
        or effective_queue_host(None)
        or "local"
    )
    return job_id, info_path, existing, queue_host


def cmd_info(args):
    # getattr, not args.job_id: the legacy `awsqueueengine info` shim declares
    # no --job-id and calls straight into here.
    token = getattr(args, "job_id", None)
    if token:
        job_id, info_path, existing, queue_host = _info_target_from_job_id(args, token)
    else:
        job_id, info_path, existing, queue_host = _info_target_from_payload(args)

    if queue_host == "local":
        state = lookup_job_state(job_id)
    else:
        # Build a small args-like shim so _rpc can use args.queue_host for messaging.
        class _RpcArgs:
            pass
        rpc_args = _RpcArgs()
        rpc_args.queue_host = queue_host
        response = _rpc(rpc_args, "job_info", {"job_id": job_id})
        state = response.get("state")
    if not state:
        print(f"Job {job_id} not found in queue host state (queued/running/completed/failed).", flush=True)
        sys.exit(1)

    merged = ledger.merge_host_fields(existing, state)
    # Update-only: looking a job up must not start tracking one this machine
    # never submitted. Also the way to re-check a job the ledger considers
    # terminal, which `jobs` skips on refresh.
    ledger.apply_state(job_id, state)

    if info_path is not None:
        write_run_info_file(info_path, merged)
        location = f"Updated {info_path}"
    else:
        location = f"Job {job_id}"
    print(
        f"{location}: status={merged.get('status', '?')} "
        f"host={merged.get('host', '-')} remote_payload={merged.get('remote_payload_path', '-')}",
        flush=True,
    )
    if merged.get("status") == "failed":
        exit_text = merged.get("exit_code") or "-"
        print(
            f"  failed: reason={merged.get('failure_reason', 'unknown')} exit={exit_text}\n"
            f"  detail: {merged.get('failure_detail') or '-'}\n"
            f"  See `awsqe-client failed --job-id {job_id} --log` for the captured log tail.",
            flush=True,
        )


def cmd_list_remote(args):
    result = _rpc(args, "list", {})
    _render_queue_jobs(result.get("jobs") or [])


def cmd_qstat_remote(args):
    result = _rpc(args, "qstat", {})
    _render_running_jobs(result.get("running") or {})


def cmd_deferred_remote(args):
    result = _rpc(args, "deferred_list", {})
    _render_deferred_jobs(result.get("jobs") or [])


def cmd_failed_remote(args):
    params = {"limit": args.limit, "log": bool(args.log)}
    if args.job_id:
        params["job_id"] = args.job_id
    result = _rpc(args, "failed_list", params)
    _render_failed_jobs(result.get("jobs") or [], show_log=bool(args.log))


def _parse_time_arg(text, flag, *, end_of_day=False):
    try:
        return parse_time_spec(text, end_of_day=end_of_day)
    except ValueError as exc:
        print(f"{flag}: {exc}", flush=True)
        sys.exit(2)


def _forget_tracked_jobs(args, records):
    """`--forget` / `--forget-before`: stop tracking, all-or-nothing."""
    conflicting = [
        flag for flag, value in (
            ("--status", args.status), ("--since", args.since), ("--until", args.until),
        ) if value
    ]
    if conflicting:
        print(
            f"--forget/--forget-before cannot be combined with {', '.join(conflicting)}; "
            "forgetting is an action, not a filter.",
            flush=True,
        )
        sys.exit(2)

    before = _parse_time_arg(args.forget_before, "--forget-before") if args.forget_before else None

    resolved = []
    missing = []
    for token in args.forget or []:
        try:
            record = ledger.find_record(token, records=records)
        except ledger.LedgerSelectionError as exc:
            print(exc.message, flush=True)
            sys.exit(1)
        if record is None:
            missing.append(token)
        else:
            resolved.append(record)
    # All-or-nothing, like qdel: a typo in one token must not half-apply.
    if missing:
        print(f"no tracked job matching: {', '.join(missing)}", flush=True)
        sys.exit(1)

    for record in resolved:
        if not is_terminal(record.get("status")):
            print(
                f"  note: {record['job_id']} is {record.get('status')}; forgetting it here "
                "does not cancel it (use `qdel`).",
                flush=True,
            )

    removed = ledger.forget(job_ids=[r["job_id"] for r in resolved], before=before)
    print(f"Forgot {removed} tracked job(s).", flush=True)


def cmd_jobs(args):
    records = ledger.load_ledger()

    if args.forget or args.forget_before:
        _forget_tracked_jobs(args, records)
        return

    try:
        statuses = expand_status_filter(args.status)
    except ValueError as exc:
        print(f"--status: {exc}", flush=True)
        sys.exit(2)
    since = _parse_time_arg(args.since, "--since") if args.since else None
    # A bare date as an upper bound means "through that day", not "up to its 00:00".
    until = _parse_time_arg(args.until, "--until", end_of_day=True) if args.until else None

    if not args.no_refresh and records:
        states, warnings = refresh_tracked_jobs(
            records,
            override_host=args.queue_host,
            default_queue_host=effective_queue_host(None),
        )
        # Ahead of the table: below a long one they scroll away, and they're
        # the reason a status might look stale.
        for warning in warnings:
            print(f"[WARN] {warning}", file=sys.stderr, flush=True)
        if states and ledger.apply_states(states):
            records = ledger.load_ledger()

    _render_tracked_jobs(
        ledger.filter_records(records, statuses=statuses, since=since,
                              until=until, limit=args.limit),
        total=len(records),
    )


def cmd_qdel_remote(args):
    selector_error = validate_qdel_selectors(args)
    if selector_error:
        print(selector_error, flush=True)
        sys.exit(1)

    job_ids, indices, queue = qdel_selectors(args)
    params = {}
    if job_ids:
        params["job_ids"] = job_ids
    if indices:
        params["indices"] = indices
    if queue:
        params["queue"] = queue

    try:
        result = rpc_call(args.queue_host, "qdel", params)
    except RpcTransportError as exc:
        print(f"RPC transport error talking to {args.queue_host}: {exc.detail}", flush=True, file=sys.stderr)
        sys.exit(1)
    except RpcError as exc:
        print(f"RPC error from {args.queue_host}: {exc.code}: {exc.message}", flush=True, file=sys.stderr)
        # A host predating job-id selectors rejects any qdel without `indices`,
        # and its message doesn't say why. Name the likely cause.
        if not indices and exc.code == "invalid_params" and "indices" in (exc.message or ""):
            print(
                f"{args.queue_host} may be running an older awsqe-host that only accepts "
                "queue positions. Upgrade it, or select with --index.",
                flush=True,
                file=sys.stderr,
            )
        sys.exit(1)

    removed = result.get("removed") or []
    if not removed:
        print("No jobs removed.", flush=True)
        return
    # Match the host CLI's qdel output so users moving between the two see the same thing.
    for entry in removed:
        idx = entry.get("index", 0)
        item = entry.get("item") or {}
        hosts_text = ",".join(item.get("hosts") or []) if item.get("hosts") else "-"
        payload_text = _payload_display_text(item)
        print(
            f"  {idx:3d}. [job={item.get('job_id') or '-'}] [priority={item.get('priority', 0)}] "
            f"[queue={item.get('queue', 'default')}] [hosts={hosts_text}] "
            f"cmd={item.get('cmd')!r} payload={payload_text!r}",
            flush=True,
        )
    print(f"Removed {len(removed)} job(s).", flush=True)
    # Record it as deleted rather than letting the next refresh see the job
    # vanish from host state and report the vaguer `missing`.
    ledger.mark_status(
        [(entry.get("item") or {}).get("job_id") for entry in removed], DELETED,
    )


def cmd_requeue_deferred_remote(args):
    if args.all and args.indices:
        print("--all cannot be combined with explicit indices.", flush=True)
        sys.exit(1)
    if not args.all and not args.indices:
        print("Provide deferred index(es) or pass --all.", flush=True)
        sys.exit(1)
    params = {
        "indices": list(args.indices),
        "all": bool(args.all),
        "drop": bool(args.drop),
    }
    result = _rpc(args, "requeue_deferred", params)
    moved = result.get("moved") or []
    action_label = "Dropped" if args.drop else "Requeued"
    if not moved:
        print("No deferred jobs were moved.", flush=True)
        return
    for entry in moved:
        idx = entry.get("index", 0)
        item = entry.get("item") or {}
        hosts_text = ",".join(item.get("hosts") or []) if item.get("hosts") else "-"
        payload_text = _payload_display_text(item)
        print(
            f"  {idx:3d}. [job={item.get('job_id') or '-'}] [priority={item.get('priority', 0)}] "
            f"[queue={item.get('queue', 'default')}] [hosts={hosts_text}] "
            f"cmd={item.get('cmd')!r} payload={payload_text!r}",
            flush=True,
        )
    print(f"{action_label} {len(moved)} deferred job(s).", flush=True)


# ---------- config subcommand handlers ----------

def cmd_config_show(args):
    cfg = load_config()
    print(f"# {CONFIG_PATH}", flush=True)
    rows = [
        ("queue_host", cfg.queue_host),
        ("s3.bucket", cfg.s3_bucket),
        ("s3.prefix", cfg.s3_prefix),
    ]
    for key, value in rows:
        display = value if value is not None else "(unset)"
        print(f"{key} = {display}", flush=True)


def cmd_config_get(args):
    try:
        key = normalize_key(args.key)
    except ValueError as exc:
        print(str(exc), flush=True)
        sys.exit(1)
    cfg = load_config()
    value = client_config.get_value(cfg, key)
    if value is None:
        sys.exit(1)
    print(value, flush=True)


def cmd_config_set(args):
    try:
        key = normalize_key(args.key)
    except ValueError as exc:
        print(str(exc), flush=True)
        sys.exit(1)
    cfg = load_config()
    set_value(cfg, key, args.value)
    path = save_config(cfg)
    # Echo what was actually persisted, not the raw input — set_value normalizes
    # some keys (e.g. s3.prefix strips whitespace and slashes).
    stored = client_config.get_value(cfg, key)
    print(f"Set {key} = {stored!r} in {path}", flush=True)


def cmd_config_unset(args):
    try:
        key = normalize_key(args.key)
    except ValueError as exc:
        print(str(exc), flush=True)
        sys.exit(1)
    cfg = load_config()
    unset_value(cfg, key)
    path = save_config(cfg)
    print(f"Unset {key} in {path}", flush=True)


def cmd_enable_host_remote(args):
    if args.all and args.hosts:
        print("--all cannot be combined with explicit host names.", flush=True)
        sys.exit(1)
    # No args → list current cooldowns; with HOST(s) or --all → release them.
    if not args.all and not args.hosts:
        result = _rpc(args, "list_cooldowns", {})
        cooldowns = result.get("cooldowns") or {}
        if not cooldowns:
            print("(no host cooldowns active)", flush=True)
            return
        print(f"{'HOST':12}  {'UNTIL':20}  REMAINING", flush=True)
        import time
        now_ts = time.time()
        for host in sorted(cooldowns):
            until_ts = float(cooldowns[host])
            until_text = format_epoch(until_ts) or "-"
            remaining_seconds = max(0, int(until_ts - now_ts))
            hours, rem = divmod(remaining_seconds, 3600)
            minutes, seconds = divmod(rem, 60)
            remaining_text = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            print(f"{host:12}  {until_text:20}  {remaining_text}", flush=True)
        return

    params = {"hosts": list(args.hosts) if args.hosts else [], "all": bool(args.all)}
    result = _rpc(args, "enable_host", params)
    cleared = result.get("cleared") or []
    if not cleared:
        if args.all:
            print("(no host cooldowns active)", flush=True)
        else:
            requested = ", ".join(args.hosts)
            print(f"No active cooldown for: {requested}", flush=True)
        return
    print(f"Released {len(cleared)} host(s) from cooldown: {', '.join(cleared)}", flush=True)


# ---------- argparse wiring ----------

def build_parser():
    parser = argparse.ArgumentParser(prog="awsqe-client", description="AWSQueueEngine client (submitter) CLI")
    sub = parser.add_subparsers(dest="cmd")

    p_status = sub.add_parser("status", help="Show status for all hosts")
    p_status.add_argument("--hosts-file", default=None)
    p_status.add_argument("--host-set", default=None)
    p_status.add_argument("--queue-host", default=None)

    p_submit = sub.add_parser("submit", help="Archive a payload, upload to S3, and enqueue on the queue host")
    p_submit.add_argument("--payload", "-p", default=None)
    p_submit.add_argument("--hosts", action="append", default=None)
    p_submit.add_argument("--host-set", default=None)
    p_submit.add_argument("--queue", default=None)
    p_submit.add_argument("--priority", type=int, default=None)
    p_submit.add_argument("--high-priority", action="store_true")
    p_submit.add_argument("--preempt", action="store_true")
    p_submit.add_argument("--mps", action="store_true", help="Wrap the command in the NVIDIA MPS launch/teardown script.")
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
    p_info.add_argument(
        "--job-id", default=None,
        help="Look up this tracked job (id or unique prefix) instead of reading "
             "run.info from a payload directory.",
    )
    p_info.add_argument("--queue-host", default=None)

    p_list = sub.add_parser("list", help="Show queued jobs on the queue host")
    p_list.add_argument("--queue-host", default=None)

    p_qstat = sub.add_parser("qstat", help="Show running jobs on the queue host")
    p_qstat.add_argument("--queue-host", default=None)

    p_deferred = sub.add_parser("deferred", help="Show deferred jobs on the queue host")
    p_deferred.add_argument("--queue-host", default=None)

    p_failed = sub.add_parser("failed", help="Show jobs that failed on the queue host")
    p_failed.add_argument("--limit", "-n", type=int, default=50, help="How many recent failures to show (default 50)")
    p_failed.add_argument("--job-id", default=None, help="Only show failures for this job id")
    p_failed.add_argument("--log", action="store_true", help="Also print the captured tail of each job log")
    p_failed.add_argument("--queue-host", default=None)

    p_jobs = sub.add_parser(
        "jobs", help="List jobs this client submitted, refreshed from the queue host",
    )
    p_jobs.add_argument(
        "--status", action="append", default=None, metavar="STATUS",
        help="Filter by status; repeatable and comma-separated. "
             f"Statuses: {', '.join(ALL_STATUSES)}. Aliases: active, done, all.",
    )
    p_jobs.add_argument(
        "--since", default=None, metavar="WHEN",
        help="Only jobs submitted at/after WHEN: YYYY-MM-DD, 'YYYY-MM-DD HH:MM', "
             "or a relative span like 30m/24h/7d/2w.",
    )
    p_jobs.add_argument(
        "--until", default=None, metavar="WHEN",
        help="Only jobs submitted before WHEN (same formats; a bare date means "
             "the end of that day).",
    )
    p_jobs.add_argument(
        "--limit", "-n", type=int, default=50,
        help="How many rows, newest first (default 50; 0 shows all)",
    )
    p_jobs.add_argument(
        "--no-refresh", action="store_true",
        help="Show recorded state without querying the queue host",
    )
    p_jobs.add_argument(
        "--forget", action="append", default=None, metavar="JOB_ID",
        help="Stop tracking these job id(s) and exit. Does NOT cancel them — use qdel.",
    )
    p_jobs.add_argument(
        "--forget-before", default=None, metavar="WHEN",
        help="Stop tracking jobs submitted before WHEN and exit.",
    )
    p_jobs.add_argument(
        "--queue-host", default=None,
        help="Refresh against this host instead of each job's recorded one",
    )

    p_qdel = sub.add_parser("qdel", help=QDEL_HELP)
    add_qdel_arguments(p_qdel)
    p_qdel.add_argument("--queue-host", default=None)

    p_requeue_deferred = sub.add_parser("requeue-deferred", help="Requeue deferred job(s) on the queue host")
    p_requeue_deferred.add_argument("indices", nargs="*", type=int, default=[])
    p_requeue_deferred.add_argument("--all", "-all", action="store_true")
    p_requeue_deferred.add_argument("--drop", action="store_true")
    p_requeue_deferred.add_argument("--queue-host", default=None)

    p_enable_host = sub.add_parser("enable-host", help="View or release host cooldowns on the queue host")
    p_enable_host.add_argument("hosts", nargs="*")
    p_enable_host.add_argument("--all", "-all", action="store_true")
    p_enable_host.add_argument("--queue-host", default=None)

    valid_keys = sorted(KEY_SCHEMA)
    p_config = sub.add_parser(
        "config",
        help="Manage ~/.awsqe/client/config.toml (persistent client settings).",
    )
    config_sub = p_config.add_subparsers(dest="config_cmd")
    config_sub.add_parser("show", help="Show all configured values")
    p_cfg_get = config_sub.add_parser("get", help=f"Print one value. Keys: {', '.join(valid_keys)}")
    p_cfg_get.add_argument("key")
    p_cfg_set = config_sub.add_parser("set", help=f"Set one value. Keys: {', '.join(valid_keys)}")
    p_cfg_set.add_argument("key")
    p_cfg_set.add_argument("value")
    p_cfg_unset = config_sub.add_parser("unset", help=f"Clear one value. Keys: {', '.join(valid_keys)}")
    p_cfg_unset.add_argument("key")

    return parser


def dispatch(args, parser=None):
    cmd = getattr(args, "cmd", None)
    if cmd == "status":
        cmd_status(args)
    elif cmd == "submit":
        if not args.command:
            print("No command provided.", flush=True)
            sys.exit(1)
        command = join_command_argv(args.command)
        if not command:
            print("No command provided.", flush=True)
            sys.exit(1)
        if args.payload:
            payload_path = Path(args.payload).expanduser()
            if not payload_path.exists():
                print(f"Payload not found on local filesystem: {payload_path}", flush=True)
                sys.exit(1)
        _resolve_queue_host(args, "submit")
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
        _resolve_queue_host(args, "list")
        cmd_list_remote(args)
    elif cmd == "qstat":
        _resolve_queue_host(args, "qstat")
        cmd_qstat_remote(args)
    elif cmd == "deferred":
        _resolve_queue_host(args, "deferred")
        cmd_deferred_remote(args)
    elif cmd == "failed":
        _resolve_queue_host(args, "failed")
        cmd_failed_remote(args)
    elif cmd == "jobs":
        # Deliberately no _resolve_queue_host: `jobs` reads a local file and
        # must work with no config and no reachable host. Each record carries
        # the host it was submitted to, resolved inside the refresh.
        cmd_jobs(args)
    elif cmd == "qdel":
        _resolve_queue_host(args, "qdel")
        cmd_qdel_remote(args)
    elif cmd == "requeue-deferred":
        _resolve_queue_host(args, "requeue-deferred")
        cmd_requeue_deferred_remote(args)
    elif cmd == "enable-host":
        _resolve_queue_host(args, "enable-host")
        cmd_enable_host_remote(args)
    elif cmd == "config":
        config_cmd = getattr(args, "config_cmd", None)
        if config_cmd == "show" or config_cmd is None:
            cmd_config_show(args)
        elif config_cmd == "get":
            cmd_config_get(args)
        elif config_cmd == "set":
            cmd_config_set(args)
        elif config_cmd == "unset":
            cmd_config_unset(args)
        else:
            print(f"Unknown config subcommand: {config_cmd}", flush=True)
            sys.exit(2)
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
