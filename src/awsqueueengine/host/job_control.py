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
        remote_cmd = (
            rf"mkdir -p {REMOTE_LOG_DIR} && cd $HOME || true && "
            rf"nohup env MANAGER_TAG={tag} bash -lc {shlex.quote(job_command)} > {REMOTE_LOG_DIR}/{tag}.log 2>&1 < /dev/null & echo $! > {REMOTE_LOG_DIR}/{tag}.pid"
        )
        rc, out, err = ssh_run(host, remote_cmd, timeout=SSH_TIMEOUT)
        if rc == 0:
            rc2, out2, err2 = ssh_run(host, f"cat {REMOTE_LOG_DIR}/{tag}.pid || true")
            pid = out2.strip() if out2 else None
            return {"host": host, "tag": tag, "pid": pid, "ok": True}
        else:
            return {"host": host, "tag": tag, "pid": None, "ok": False, "err": err or out, "reason": "host_transport"}
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
    remote_cmd = (
        rf"mkdir -p {REMOTE_LOG_DIR} && cd $HOME || true && "
        rf"nohup env MANAGER_TAG={tag} PAYLOAD_DIR={remote_payload_dir} bash -lc {shlex.quote(job_command)} "
        rf"> {REMOTE_LOG_DIR}/{tag}.log 2>&1 < /dev/null & echo $! > {REMOTE_LOG_DIR}/{tag}.pid"
    )
    rc, out, err = ssh_run(host, remote_cmd, timeout=SSH_TIMEOUT)
    rc2, pid_out, err2 = ssh_run(host, f"cat {REMOTE_LOG_DIR}/{tag}.pid || true", timeout=5)
    pid = pid_out.strip() if pid_out else None
    if pid:
        rc3, ps_out, err3 = ssh_run(host, f"ps -p {pid} -o pid= || true", timeout=5)
        if ps_out.strip():
            return {"host": host, "tag": tag, "pid": pid, "ok": True, "payload": remote_payload_dir}
        else:
            return {"host": host, "tag": tag, "pid": pid, "ok": False, "err": "pidfile present but process not running", "payload": remote_payload_dir, "reason": "job"}
    rc4, out4, _ = ssh_run(host, "ps -eo pid,cmd | grep -F 'MANAGER_TAG=' | grep -v grep || true", timeout=8)
    if out4:
        try:
            pid_guess = out4.splitlines()[0].split(None, 1)[0]
            return {"host": host, "tag": tag, "pid": pid_guess, "ok": True, "payload": remote_payload_dir, "note": "started-no-pidfile"}
        except Exception:
            pass
    return {"host": host, "tag": tag, "pid": None, "ok": False, "err": err or out, "payload": remote_payload_dir, "reason": "job"}


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
