"""Host-side job placement on worker hosts.

`submit_to_host` is the workhorse called by the monitor. `write_run_info`
writes the run.info file in the submitter's payload directory.
"""
import shlex
from pathlib import Path
from urllib.parse import urlparse

from ..shared.config import REMOTE_LOG_DIR, SSH_TIMEOUT
from ..shared.job_outcome import rc_path_for_tag
from ..shared.ssh_utils import ssh_run
from ..shared.worker_actions import new_job_tag
from ..shared.worker_staging import (
    choose_scratch_on_host,
    rsync_to_host_with_fallback,
    sizeof_local_path_bytes,
)


def _payload_name_from_s3_uri(payload_s3_uri):
    parsed = urlparse(payload_s3_uri)
    name = Path(parsed.path).name
    if name.endswith(".tar.gz"):
        name = name[:-7]
    return name or "payload"


# Jobs that opt into MPS (``--mps``) have their command bracketed by this
# template before launch. MPS control has to be (re)started per job on these
# hosts, so we tear down any stale daemon, start a fresh one with per-job
# pipe/log directories, run the job in its place, quit the daemon, then remove
# the per-job /tmp dir so MPS scratch doesn't accumulate on long-lived workers.
MPS_WRAPPER_TEMPLATE = """\
killall nvidia-cuda-mps-control nvidia-cuda-mps-server 2>/dev/null || true
sleep 1
job_name={job_name}
temp_path=/tmp/temp_${{job_name}}
mkdir -p ${{temp_path}}
export CUDA_MPS_PIPE_DIRECTORY=${{temp_path}}/nvidia-mps
export CUDA_MPS_LOG_DIRECTORY=${{temp_path}}/nvidia-log
nvidia-cuda-mps-control -d
sleep 1

{job_command}
__awsqe_job_rc=$?

echo quit | nvidia-cuda-mps-control
rm -rf ${{temp_path}}
exit $__awsqe_job_rc
"""


def wrap_in_mps_script(job_command, job_name):
    """Bracket ``job_command`` with NVIDIA MPS launch/teardown boilerplate.

    ``job_name`` (the job tag) keeps the MPS pipe/log directories unique per
    job under ``/tmp``. The result is a multi-line bash script meant to be run
    via ``bash -lc`` exactly where the bare command would have been.
    """
    safe_job_name = shlex.quote(str(job_name) if job_name else "awsqe")
    return MPS_WRAPPER_TEMPLATE.format(job_name=safe_job_name, job_command=job_command)


# Every job is bracketed by this so the worker records how it ended. The
# monitor reads `{tag}.rc` back when the host goes idle (see
# `awsqueueengine.shared.job_outcome`); without it a job that dies in two
# seconds is indistinguishable from one that finished cleanly. The leading
# `rm -f` matters for requeued jobs, which reuse their job tag: a stale status
# from the previous attempt must not be read back as this attempt's outcome.
#
# The status is written from an EXIT trap rather than after the command, so a
# job script that ends in `exit 1` (which would otherwise leave the shell before
# any trailing line ran) still records what happened. A job killed outright —
# preemption, SIGKILL, host reboot — records nothing, and the monitor reports
# that as `no_exit_status` instead of a clean finish.
EXIT_STATUS_WRAPPER_TEMPLATE = """\
__awsqe_rc_path={rc_path}
rm -f "$__awsqe_rc_path"
trap '__awsqe_rc=$?; echo $__awsqe_rc > "$__awsqe_rc_path" 2>/dev/null || true' EXIT
{job_command}
"""


def wrap_with_exit_status(job_command, tag):
    """Bracket ``job_command`` so its exit status lands in ``{tag}.rc``."""
    return EXIT_STATUS_WRAPPER_TEMPLATE.format(
        rc_path=shlex.quote(rc_path_for_tag(tag)),
        job_command=job_command,
    )


# The launch has to hand the job off and return, not babysit it. Two details
# earn their keep:
#
#   * every command sits on its own line, so `&` terminates only the `nohup`.
#     Written as a `&&` chain with a trailing `&`, the ampersand binds looser
#     than `&&` and backgrounds the *whole* chain; the resulting subshell
#     inherits ssh's stdout/stderr and holds the channel open until the job
#     exits. That made every launch block for the job's full runtime, so any
#     job outliving SSH_TIMEOUT came back as `ssh timeout` (issue #33).
#   * `setsid` puts the job in its own session, so sshd has nothing left to
#     wait on once the foreground shell hits `exit 0`.
#
# `$!` is now the job's own PID rather than a wrapper subshell's, and `mkdir`
# is sequenced before the `echo` that writes the pidfile into it.
LAUNCH_SCRIPT_TEMPLATE = """\
mkdir -p {log_dir}
cd "$HOME" || true
nohup setsid env MANAGER_TAG={tag}{payload_env} bash -lc {job_command} > {log_dir}/{tag}.log 2>&1 < /dev/null &
__awsqe_pid=$!
echo "$__awsqe_pid" > {log_dir}/{tag}.pid
exit 0
"""


def _build_launch_command(tag, job_command, remote_payload_dir=None):
    """Build the remote script that starts ``job_command`` and returns at once."""
    payload_env = f" PAYLOAD_DIR={remote_payload_dir}" if remote_payload_dir else ""
    return LAUNCH_SCRIPT_TEMPLATE.format(
        log_dir=REMOTE_LOG_DIR,
        tag=tag,
        payload_env=payload_env,
        job_command=shlex.quote(job_command),
    )


def _verify_launched(host, tag, remote_payload_dir=None, err=None):
    """Decide whether a launch took, without trusting the launch ssh's status.

    The launch ssh can report failure for reasons that have nothing to do with
    the host — chiefly a timeout on a job that started perfectly well. So ask
    the host what actually happened: read the pidfile, confirm the process is
    alive, and fall back to scanning for the MANAGER_TAG marker if the pidfile
    never landed. A pidfile whose process is gone means the job ran and died —
    that is ``job``, not the host's fault.

    When nothing is found at all, the verdict turns on whether the host
    answered us. Both probes end in ``|| true``, so a zero status means the
    host ran them and genuinely has no such process (``job``); a non-zero one
    means ssh never got through (``host_transport``). Only that second case
    costs the host a cooldown, so a host is blamed for being unreachable and
    nothing else.
    """
    result = {"host": host, "tag": tag, "pid": None}
    if remote_payload_dir:
        result["payload"] = remote_payload_dir
    pid_rc, pid_out, _ = ssh_run(host, f"cat {REMOTE_LOG_DIR}/{tag}.pid || true", timeout=5)
    pid = pid_out.strip() if pid_out else None
    if pid:
        result["pid"] = pid
        _, ps_out, _ = ssh_run(host, f"ps -p {pid} -o pid= || true", timeout=5)
        if ps_out.strip():
            return {**result, "ok": True}
        return {**result, "ok": False, "err": "pidfile present but process not running", "reason": "job"}
    scan_rc, scan_out, _ = ssh_run(host, "ps -eo pid,cmd | grep -F 'MANAGER_TAG=' | grep -v grep || true", timeout=8)
    if scan_out:
        try:
            pid_guess = scan_out.splitlines()[0].split(None, 1)[0]
            return {**result, "pid": pid_guess, "ok": True, "note": "started-no-pidfile"}
        except Exception:
            pass
    host_answered = pid_rc == 0 and scan_rc == 0
    reason = "job" if host_answered else "host_transport"
    return {**result, "ok": False, "err": err, "reason": reason}


def submit_to_host(
    host,
    job_command,
    payload_local_path=None,
    payload_remote_path=None,
    payload_s3_uri=None,
    payload_size_bytes=None,
    tag=None,
    mps=False,
):
    tag = tag or new_job_tag()
    if mps:
        job_command = wrap_in_mps_script(job_command, job_name=tag)
    job_command = wrap_with_exit_status(job_command, tag)
    remote_payload_dir = None
    if payload_remote_path:
        remote_payload_dir = str(payload_remote_path).strip() or None
    if not payload_local_path and not remote_payload_dir and not payload_s3_uri:
        remote_cmd = _build_launch_command(tag, job_command)
        rc, out, err = ssh_run(host, remote_cmd, timeout=SSH_TIMEOUT)
        return _verify_launched(host, tag, err=err or out)
    if payload_s3_uri and not remote_payload_dir:
        try:
            needed_bytes = int(payload_size_bytes or 0)
        except (TypeError, ValueError):
            needed_bytes = 0
        remote_root, info = choose_scratch_on_host(host, needed_bytes)
        if not remote_root:
            return {"host": host, "ok": False, "err": f"no suitable scratch: {info}", "reason": "host_storage"}
        jobname = _payload_name_from_s3_uri(payload_s3_uri)
        remote_payload_dir = f"{remote_root}/{jobname}-{tag}"
        archive_path = f"{remote_payload_dir}/payload.tar.gz"
        download_cmd = "\n".join(
            [
                "set -euo pipefail",
                f"mkdir -p {shlex.quote(remote_payload_dir)}",
                f"chmod 700 {shlex.quote(remote_payload_dir)}",
                f"aws s3 cp {shlex.quote(payload_s3_uri)} {shlex.quote(archive_path)}",
                f"tar xzf {shlex.quote(archive_path)} -C {shlex.quote(remote_payload_dir)}",
                f"rm -f {shlex.quote(archive_path)}",
            ]
        )
        rc, out, err = ssh_run(host, f"bash -lc {shlex.quote(download_cmd)}", timeout=900)
        if rc != 0:
            return {
                "host": host,
                "tag": tag,
                "ok": False,
                "err": f"s3 payload download failed: {err or out}",
                "payload": remote_payload_dir,
                "reason": "host_transport",
            }
    if not remote_payload_dir:
        local_path = Path(payload_local_path).expanduser()
        if not local_path.exists():
            return {"host": host, "ok": False, "err": f"local payload not found: {local_path}", "reason": "job"}
        needed_bytes = sizeof_local_path_bytes(local_path)
        remote_root, info = choose_scratch_on_host(host, needed_bytes)
        if not remote_root:
            return {"host": host, "ok": False, "err": f"no suitable scratch: {info}", "reason": "host_storage"}
        jobname = Path(payload_local_path).name
        remote_payload_dir = f"{remote_root}/{jobname}-{tag}"
        rc, out, err = ssh_run(host, f"mkdir -p {remote_root} && chmod 700 {remote_root}", timeout=30)
        if rc != 0:
            return {"host": host, "ok": False, "err": f"mkdir failed: {err or out}", "payload": remote_payload_dir, "reason": "host_transport"}
        ok, method, sout, serr = rsync_to_host_with_fallback(str(local_path), host, remote_payload_dir)
        if not ok:
            return {"host": host, "ok": False, "err": f"rsync failed: {serr or sout}", "reason": "host_transport"}
    remote_cmd = _build_launch_command(tag, job_command, remote_payload_dir)
    rc, out, err = ssh_run(host, remote_cmd, timeout=SSH_TIMEOUT)
    return _verify_launched(host, tag, remote_payload_dir, err=err or out)


def write_run_info(local_payload_path, jobid, host, remote_payload_path):
    if not local_payload_path:
        return
    p = Path(local_payload_path).expanduser()
    try:
        p.mkdir(parents=True, exist_ok=True)
        info_path = p / "run.info"
        info_path.write_text(f"{jobid}\n{host}\n{remote_payload_path}\n")
    except Exception as e:
        print(f"[WARN] failed to write run.info in {p}: {e}")
