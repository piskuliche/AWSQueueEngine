# CLI interface for AWSQueueManager
import sys
import argparse
from .config import HOSTS
from .queue import enqueue_item, load_queue, save_queue
from .host_status import status_all
from .monitor import acquire_monitor_lock, release_monitor_lock, monitor_loop
from .job_control import submit_to_host, tail_remote_log, kill_managed_on_host
from .staging import where_is_next_submit

def main():
    parser = argparse.ArgumentParser(description="Simple Slurm-like manager for SSH GPU hosts.")
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("status", help="Show status for all hosts")
    p_submit = sub.add_parser("submit", help="Enqueue a job (command string)")
    p_submit.add_argument("--payload", "-p", help="Local folder to copy to remote scratch before running", default=None)
    p_submit.add_argument("command", nargs=argparse.REMAINDER, help="Command to run remotely (quoted)")

    sub.add_parser("list", help="Show queued jobs")
    sub.add_parser("clear", help="Clear the queue")
    sub.add_parser("start", help="Start monitor loop (runs until Ctrl-C)")
    sub.add_parser("where", help="Show where the next job will be submitted")

    p_tail = sub.add_parser("tail", help="Tail remote log on a host")
    p_tail.add_argument("host")

    p_stop = sub.add_parser("stop", help="Kill managed job(s) on a host")
    p_stop.add_argument("host")

    args = parser.parse_args()
    print("Starting the queue engine")

    if args.cmd == "status":
        rows = status_all(HOSTS)
        print(f"{'HOST':8}  {'REACH':8}  {'PID':8}  {'TAG':12}  INFO")
        for r in rows:
            reach = "yes" if r["reachable"] else "no"
            pid = r["pid"] or "-"
            tag = r["tag"] or "-"
            info = (r["raw"][:60] + "...") if r["raw"] else ""
            print(f"{r['host']:8}  {reach:8}  {pid:8}  {tag:12}  {info}")
    elif args.cmd == "submit":
        if not args.command:
            print("No command provided.")
            sys.exit(1)
        command = " ".join(args.command).strip()
        item = {"cmd": command, "payload": args.payload}
        enqueue_item(item)
        print("Enqueued:", item)
    elif args.cmd == "list":
        q = load_queue()
        if not q:
            print("(queue empty)")
        else:
            for i,cmd in enumerate(q,1):
                print(f"{i:3d}. {cmd}")
    elif args.cmd == "clear":
        save_queue([])
        print("Queue cleared.")
    elif args.cmd == "start":
        fd, holder = acquire_monitor_lock()
        if fd is None:
            print(f"Monitor already running (holder={holder})")
            sys.exit(1)
        try:
            monitor_loop(HOSTS)
        finally:
            release_monitor_lock(fd)
    elif args.cmd == "tail":
        r = tail_remote_log(args.host)
        if not r["ok"]:
            print("Error:", r.get("reason") or r.get("err"))
        else:
            header = f"Host: {r['host']}  tag: {r.get('tag') or '(none)'}"
            print(header)
            print("-"*len(header))
            print(r.get("out") or "(no log output)")
    elif args.cmd == "stop":
        res = kill_managed_on_host(args.host)
        if res["rc"] == 0:
            print(f"Sent kill to managed job(s) on {args.host}.")
        else:
            print("Kill error:", res.get("err") or res.get("out"))
    elif args.cmd == "where":
        where_is_next_submit()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
