"""Legacy ``awsqueueengine`` CLI shim.

Phase 1 keeps full backward compatibility: every subcommand the original
CLI accepted still works. Internally this just builds the union argparser
and dispatches to the per-side handlers in :mod:`awsqueueengine.client.cli`
and :mod:`awsqueueengine.host.cli`. No DeprecationWarning is emitted yet; a
future release will add one and point users at ``awsqe-client`` /
``awsqe-host``, which are the supported entry points today.

This is the one module allowed to import from both the client and host
subpackages. Code inside ``client/*`` and ``host/*`` must not cross.
"""
import argparse
import sys
from pathlib import Path

from .client import cli as client_cli
from .client.config import effective_queue_host
from .host import cli as host_cli
from .shared.cli_utils import QDEL_HELP, add_qdel_arguments, join_command_argv


def _build_parser():
    parser = argparse.ArgumentParser(description="Simple Slurm-like manager for SSH GPU hosts.")
    parser.add_argument(
        "--test-email-connection",
        action="store_true",
        help="Send a test notification email through Mailtrap and exit.",
    )
    sub = parser.add_subparsers(dest="cmd")

    p_status = sub.add_parser("status", help="Show status for all hosts")
    p_status.add_argument("--hosts-file", default=None)
    p_status.add_argument("--host-set", default=None)

    p_qstat = sub.add_parser("qstat", help="Show running jobs tracked by monitor")
    p_qstat.add_argument("--queue-host", default=None)

    p_submit = sub.add_parser("submit", help="Enqueue a job (command string)")
    p_submit.add_argument("--hosts-file", default=None)
    p_submit.add_argument("--payload", "-p", default=None)
    p_submit.add_argument("--hosts", action="append", default=None)
    p_submit.add_argument("--host-set", default=None, help="Deprecated alias for --queue.")
    p_submit.add_argument("--queue", default=None)
    p_submit.add_argument("--priority", type=int, default=None)
    p_submit.add_argument("--high-priority", action="store_true")
    p_submit.add_argument("--preempt", action="store_true")
    p_submit.add_argument("--mps", action="store_true", help="Wrap the command in the NVIDIA MPS launch/teardown script.")
    p_submit.add_argument(
        "--array", default=None, metavar="NAME",
        help="Tag this job as part of batch NAME (see `awsqe-client jobs`).",
    )
    p_submit.add_argument("--queue-host", default=None)
    p_submit.add_argument("--payload-s3-uri", default=None, help=argparse.SUPPRESS)
    p_submit.add_argument("--payload-size-bytes", type=int, default=None, help=argparse.SUPPRESS)
    p_submit.add_argument("--job-id", default=None, help=argparse.SUPPRESS)
    p_submit.add_argument("command", nargs=argparse.REMAINDER)

    p_requeue = sub.add_parser("requeue-running", help="Kill running managed job(s) and requeue at priority 100")
    p_requeue.add_argument("--hosts-file", default=None)
    p_requeue.add_argument(
        "--mps",
        action="store_true",
        help="Force the NVIDIA MPS wrapper on the requeued job(s) (default: keep each job's current setting).",
    )
    target = p_requeue.add_mutually_exclusive_group(required=True)
    target.add_argument("--hosts", action="append", default=None)
    target.add_argument("--all", "-all", action="store_true")

    p_list = sub.add_parser("list", help="Show queued jobs")
    p_list.add_argument("--queue-host", default=None)

    p_qdel = sub.add_parser("qdel", help=QDEL_HELP)
    add_qdel_arguments(p_qdel)

    sub.add_parser("clear", help="Clear the queue")

    p_deferred = sub.add_parser("deferred", help="Show deferred jobs")
    p_deferred.add_argument("--queue-host", default=None)

    p_requeue_deferred = sub.add_parser("requeue-deferred", help="Move deferred job(s) back into the main queue")
    p_requeue_deferred.add_argument("indices", nargs="*", type=int, default=[])
    p_requeue_deferred.add_argument("--all", "-all", action="store_true")
    p_requeue_deferred.add_argument("--drop", action="store_true")
    p_requeue_deferred.add_argument("--queue-host", default=None)

    p_enable_host = sub.add_parser("enable-host", help="Show active host cooldowns; with HOST(s) or --all, release them early")
    p_enable_host.add_argument("hosts", nargs="*")
    p_enable_host.add_argument("--all", "-all", action="store_true")
    p_enable_host.add_argument("--queue-host", default=None)

    sub.add_parser("start", help=argparse.SUPPRESS)  # unimplemented legacy stub
    sub.add_parser("where", help="Show where the next job will be submitted")

    p_start_monitor = sub.add_parser("start-monitor", help="Start the monitor loop (daemon mode)")
    p_start_monitor.add_argument("--hosts-file", default=None)
    sub.add_parser("stop-monitor", help="Stop the running monitor loop")
    sub.add_parser("status-monitor", help="Show monitor status")

    p_tail = sub.add_parser("tail", help="Tail remote log on a host")
    p_tail.add_argument("host")

    p_stop = sub.add_parser("stop", help="Kill managed job(s) on a host")
    p_stop.add_argument("host")

    p_info = sub.add_parser("info", help="Refresh local run.info from queue host state")
    p_info.add_argument("--payload", "-p", default=None)
    p_info.add_argument("--queue-host", default=None)

    p_job_info = sub.add_parser("job-info", help="Emit JSON state for a job_id from local queue/running/completed")
    p_job_info.add_argument("job_id")

    return parser


def main():
    sys.stdout = open(sys.stdout.fileno(), mode='w', buffering=1, encoding=sys.stdout.encoding, closefd=False)
    sys.stderr = open(sys.stderr.fileno(), mode='w', buffering=1, encoding=sys.stderr.encoding, closefd=False)

    parser = _build_parser()
    args = parser.parse_args()

    if args.test_email_connection:
        host_cli.cmd_test_email()
        return

    # For the dual subcommands that take --queue-host, fall back to the
    # client config's queue_host when the flag isn't on the CLI. If neither
    # is set, args.queue_host stays None and the command runs locally.
    if hasattr(args, "queue_host"):
        args.queue_host = effective_queue_host(getattr(args, "queue_host", None))

    cmd = args.cmd

    # CLIENT-only subcommands
    if cmd == "status":
        client_cli.cmd_status(args)
        return
    if cmd == "tail":
        client_cli.cmd_tail(args)
        return
    if cmd == "stop":
        client_cli.cmd_stop(args)
        return
    if cmd == "where":
        client_cli.cmd_where(args)
        return

    # HOST-only subcommands
    if cmd == "qdel":
        host_cli.cmd_qdel(args)
        return
    if cmd == "clear":
        host_cli.cmd_clear(args)
        return
    if cmd == "requeue-running":
        host_cli.cmd_requeue_running(args)
        return
    if cmd == "job-info":
        host_cli.cmd_job_info(args)
        return
    if cmd == "start-monitor":
        host_cli.cmd_monitor(args)
        return
    if cmd == "stop-monitor":
        sys.exit(host_cli.cmd_stop_monitor(args))
    if cmd == "status-monitor":
        sys.exit(host_cli.cmd_status_monitor(args))

    # DUAL subcommands: --queue-host routes to client; otherwise host.
    if cmd == "submit":
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
        command = join_command_argv(args.command)
        if not command:
            print("No command provided.", flush=True)
            sys.exit(1)
        if args.payload:
            payload_path = Path(args.payload).expanduser()
            if not payload_path.exists():
                print(f"Payload not found on local filesystem: {payload_path}", flush=True)
                sys.exit(1)
        if args.queue_host:
            client_cli.cmd_submit_remote(args, command)
        else:
            host_cli.cmd_submit_local(args, command)
        return

    if cmd == "qstat":
        if args.queue_host:
            client_cli.cmd_qstat_remote(args)
        else:
            host_cli.cmd_qstat(args)
        return
    if cmd == "list":
        if args.queue_host:
            client_cli.cmd_list_remote(args)
        else:
            host_cli.cmd_list(args)
        return
    if cmd == "deferred":
        if args.queue_host:
            client_cli.cmd_deferred_remote(args)
        else:
            host_cli.cmd_deferred(args)
        return
    if cmd == "requeue-deferred":
        if args.queue_host:
            client_cli.cmd_requeue_deferred_remote(args)
        else:
            host_cli.cmd_requeue_deferred(args)
        return
    if cmd == "enable-host":
        if args.queue_host:
            client_cli.cmd_enable_host_remote(args)
        else:
            host_cli.cmd_enable_host(args)
        return
    if cmd == "info":
        client_cli.cmd_info(args)
        return

    parser.print_help()


if __name__ == "__main__":
    main()
