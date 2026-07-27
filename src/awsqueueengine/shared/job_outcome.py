"""Read back how a job ended on a worker host, and label the failure.

Every managed job is launched wrapped so that its exit status lands in
``{REMOTE_LOG_DIR}/{tag}.rc`` (see :func:`awsqueueengine.host.job_control.submit_to_host`).
Once the monitor sees a host go idle it calls :func:`fetch_job_outcome` to
pick that status up, along with the tail of the job log, and
:func:`classify_failure` turns the pair into a short, human-readable reason.

Jobs launched before the ``.rc`` wrapper existed (or killed hard enough that
the wrapper never ran) leave no status file; those come back as
``no_exit_status`` rather than being silently counted as successes.
"""
import re
import shlex

from .config import REMOTE_LOG_DIR
from .ssh_utils import ssh_run

# Sentinel the fetch script prints when the .rc file is absent, so a missing
# status is distinguishable from an empty/garbled one.
_MISSING = "__awsqe_no_rc__"

LOG_TAIL_LINES = 40
# run.info is one key:value pair per line, so the short detail we surface to
# the client has to stay single-line and small.
MAX_DETAIL_CHARS = 200

# Log-tail patterns checked before the exit code, most specific first. These
# beat the numeric code because "exit 1 with a CUDA OOM in the log" is a far
# more useful answer than "exit 1".
_LOG_PATTERNS = [
    ("out_of_memory", re.compile(r"out of memory|oom-kill|oom killer|MemoryError|Cannot allocate memory", re.I)),
    ("disk_full", re.compile(r"No space left on device|Disk quota exceeded", re.I)),
    ("cuda_error", re.compile(r"CUDA error|CUDA_ERROR|no CUDA-capable device|cudaError|NVIDIA-SMI has failed", re.I)),
    ("command_not_found", re.compile(r"command not found", re.I)),
    ("permission_denied", re.compile(r"Permission denied", re.I)),
    ("python_import_error", re.compile(r"ModuleNotFoundError|ImportError", re.I)),
    ("python_exception", re.compile(r"Traceback \(most recent call last\)", re.I)),
]

# Fallbacks keyed on the exit status itself.
_EXIT_CODE_REASONS = {
    126: "not_executable",
    127: "command_not_found",
    130: "interrupted",
    134: "aborted",
    137: "killed",
    139: "segfault",
    143: "terminated",
}


def rc_path_for_tag(tag):
    return f"{REMOTE_LOG_DIR}/{tag}.rc"


def log_path_for_tag(tag):
    return f"{REMOTE_LOG_DIR}/{tag}.log"


def fetch_job_outcome(host, tag, lines=LOG_TAIL_LINES, ssh_run=ssh_run):
    """Return ``{exit_code, log_tail, found, error}`` for a finished job.

    ``exit_code`` is ``None`` when the worker recorded no status. ``found`` is
    False when the status file was missing or the host could not be reached.
    """
    outcome = {"exit_code": None, "log_tail": "", "found": False, "error": ""}
    if not tag:
        outcome["error"] = "no job tag recorded"
        return outcome
    try:
        lines_n = int(lines)
    except (TypeError, ValueError):
        lines_n = LOG_TAIL_LINES
    lines_n = max(1, min(lines_n, 500))

    rc_file = shlex.quote(rc_path_for_tag(tag))
    log_file = shlex.quote(log_path_for_tag(tag))
    # One round trip: exit status on the first line, log tail after a marker.
    cmd = (
        f"if [ -f {rc_file} ]; then cat {rc_file}; else echo {_MISSING}; fi; "
        f"echo '__awsqe_log__'; "
        f"tail -n {lines_n} {log_file} 2>/dev/null || true"
    )
    rc, out, err = ssh_run(host, cmd)
    if rc != 0 and not out:
        outcome["error"] = (err or "ssh failed").strip()
        return outcome

    head, _, tail = (out or "").partition("__awsqe_log__\n")
    outcome["log_tail"] = tail.strip()
    status_text = head.strip().splitlines()
    status_text = status_text[0].strip() if status_text else ""
    if not status_text or status_text == _MISSING:
        outcome["error"] = "no exit status recorded on host"
        return outcome
    try:
        outcome["exit_code"] = int(status_text)
    except ValueError:
        outcome["error"] = f"unparseable exit status: {status_text[:40]!r}"
        return outcome
    outcome["found"] = True
    return outcome


def _last_matching_line(log_tail, pattern):
    for line in reversed((log_tail or "").splitlines()):
        if pattern.search(line):
            return line.strip()
    return ""


def classify_failure(exit_code, log_tail="", error=""):
    """Return ``(reason, detail)`` for a failed job.

    ``reason`` is a short stable slug (``out_of_memory``, ``segfault``, ...)
    meant for grouping; ``detail`` is a single-line human hint, usually the
    offending log line.
    """
    for reason, pattern in _LOG_PATTERNS:
        line = _last_matching_line(log_tail, pattern)
        if line:
            return reason, line[:MAX_DETAIL_CHARS]

    if exit_code is None:
        detail = error or "job ended without recording an exit status (killed, host reboot, or manual stop)"
        return "no_exit_status", detail[:MAX_DETAIL_CHARS]

    reason = _EXIT_CODE_REASONS.get(exit_code)
    if reason is None and exit_code > 128:
        reason = f"signal_{exit_code - 128}"
    if reason is None:
        reason = "nonzero_exit"

    last_line = ""
    for line in reversed((log_tail or "").splitlines()):
        if line.strip():
            last_line = line.strip()
            break
    detail = last_line or f"exited with status {exit_code}"
    return reason, detail[:MAX_DETAIL_CHARS]
