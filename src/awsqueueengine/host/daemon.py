"""systemd-style host daemon management.

Provides install/uninstall/start/stop/restart/status/logs verbs for
running the queue-host monitor under systemd, with a foreground fallback
when systemd isn't available (dev, CI, macOS, …).

Two installation modes:

- **System unit** (default; requires sudo). Unit lives at
  ``/etc/systemd/system/awsqe-host.service``; ``WantedBy=multi-user.target``;
  ``User=`` is set to ``$SUDO_USER`` so the daemon owns the same
  ``~/.aws_slurm_like_*.json`` (or, after Phase 5, ``~/.awsqe/host/``) state
  files the operator already has.

- **User unit** (``--user``; no root needed). Unit lives under
  ``~/.config/systemd/user/awsqe-host.service``; ``WantedBy=default.target``;
  the daemon runs as the invoking user. Requires that user lingering is
  enabled if the daemon should run while the user is logged out
  (``loginctl enable-linger <user>``) — we print a hint.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

SERVICE_NAME = "awsqe-host"
UNIT_FILENAME = f"{SERVICE_NAME}.service"

SYSTEM_UNIT_PATH = Path("/etc/systemd/system") / UNIT_FILENAME
USER_UNIT_DIR = Path.home() / ".config" / "systemd" / "user"
USER_UNIT_PATH = USER_UNIT_DIR / UNIT_FILENAME


UNIT_TEMPLATE = """\
[Unit]
Description=AWSQueueEngine queue-host monitor
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={exec_start}
Restart=on-failure
RestartSec=5
{user_line}
[Install]
WantedBy={wanted_by}
"""


@dataclass(frozen=True)
class UnitPlan:
    """A concrete decision about where the unit goes and how to run systemctl."""
    user_mode: bool
    unit_path: Path
    systemctl_args: tuple[str, ...]   # base argv for systemctl in this mode
    journalctl_args: tuple[str, ...]  # base argv for journalctl in this mode
    run_as_user: str | None           # User= field for system units; None for user mode
    wanted_by: str


# ---------- mode resolution ----------

def resolve_plan(user_mode: bool) -> UnitPlan:
    if user_mode:
        return UnitPlan(
            user_mode=True,
            unit_path=USER_UNIT_PATH,
            systemctl_args=("systemctl", "--user"),
            journalctl_args=("journalctl", "--user"),
            run_as_user=None,
            wanted_by="default.target",
        )
    # System mode: figure out which user the daemon should run as.
    sudo_user = os.environ.get("SUDO_USER", "").strip()
    if sudo_user and sudo_user != "root":
        run_as = sudo_user
    elif os.environ.get("USER") and os.environ["USER"] != "root":
        run_as = os.environ["USER"]
    else:
        run_as = "ubuntu"  # AWS Ubuntu AMI convention; user can edit the unit if wrong
    return UnitPlan(
        user_mode=False,
        unit_path=SYSTEM_UNIT_PATH,
        systemctl_args=("systemctl",),
        journalctl_args=("journalctl",),
        run_as_user=run_as,
        wanted_by="multi-user.target",
    )


def systemctl_available() -> bool:
    return shutil.which("systemctl") is not None


# ---------- unit-file rendering ----------

def resolve_exec_start() -> str:
    """Pick the command systemd should run for the monitor.

    Prefer the installed ``awsqe-host`` console script (so a venv reinstall
    keeps working without re-editing the unit). Fall back to invoking the
    current Python interpreter against the host CLI module.
    """
    found = shutil.which(SERVICE_NAME)
    if found:
        return f"{found} monitor"
    return f"{sys.executable} -m awsqueueengine.host.cli monitor"


def render_unit(plan: UnitPlan, *, exec_start: str | None = None) -> str:
    user_line = ""
    if not plan.user_mode and plan.run_as_user:
        user_line = f"User={plan.run_as_user}\n"
    return UNIT_TEMPLATE.format(
        exec_start=exec_start or resolve_exec_start(),
        user_line=user_line,
        wanted_by=plan.wanted_by,
    )


# ---------- low-level helpers ----------

def _run(argv, *, dry_run: bool, check: bool = False, capture_output: bool = False):
    """Run a subprocess, or print what would have run under --dry-run."""
    if dry_run:
        print(f"[dry-run] {' '.join(argv)}", flush=True)
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")
    return subprocess.run(argv, check=check, text=True, capture_output=capture_output)


def _write_unit(plan: UnitPlan, content: str, *, dry_run: bool) -> None:
    if dry_run:
        print(f"[dry-run] write unit to {plan.unit_path}:", flush=True)
        for line in content.splitlines():
            print(f"  | {line}", flush=True)
        return
    plan.unit_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = plan.unit_path.with_suffix(plan.unit_path.suffix + ".tmp")
    tmp.write_text(content)
    os.replace(tmp, plan.unit_path)


def _remove_unit(plan: UnitPlan, *, dry_run: bool) -> bool:
    if not plan.unit_path.exists() and not dry_run:
        return False
    if dry_run:
        print(f"[dry-run] rm {plan.unit_path}", flush=True)
        return True
    plan.unit_path.unlink()
    return True


def _system_mode_needs_root(plan: UnitPlan) -> bool:
    """Return True if this is a system-mode op that requires root but isn't running as it.

    Guarded with ``hasattr(os, "geteuid")`` so the module imports cleanly on
    Windows (where systemctl wouldn't be available anyway, so the verbs would
    already have errored out earlier on missing systemctl).
    """
    if plan.user_mode:
        return False
    if not hasattr(os, "geteuid"):
        return False
    return os.geteuid() != 0


def _print_root_required() -> None:
    print(
        "System install requires root. Re-run with sudo, or pass --user "
        "to install a per-user unit at ~/.config/systemd/user/.",
        flush=True,
        file=sys.stderr,
    )


# ---------- public verbs ----------

def install(*, user_mode: bool, force: bool, dry_run: bool) -> int:
    if not systemctl_available():
        print(
            "systemctl was not found on PATH. Install systemd or use "
            "`awsqe-host monitor` to run the daemon in the foreground.",
            flush=True,
            file=sys.stderr,
        )
        return 1
    plan = resolve_plan(user_mode)
    if _system_mode_needs_root(plan):
        _print_root_required()
        return 1
    if plan.unit_path.exists() and not force:
        print(
            f"Unit already exists at {plan.unit_path}. Pass --force to overwrite.",
            flush=True,
            file=sys.stderr,
        )
        return 1
    content = render_unit(plan)
    _write_unit(plan, content, dry_run=dry_run)
    _run([*plan.systemctl_args, "daemon-reload"], dry_run=dry_run)
    _run([*plan.systemctl_args, "enable", "--now", SERVICE_NAME], dry_run=dry_run)
    location = "user" if plan.user_mode else "system"
    prefix = "[dry-run] would install" if dry_run else "Installed"
    suffix = "" if dry_run else f" and started {SERVICE_NAME}"
    print(f"{prefix} {location} unit at {plan.unit_path}{suffix}.", flush=True)
    if plan.user_mode:
        print(
            "Tip: run `loginctl enable-linger $USER` to keep the daemon running "
            "while you are logged out.",
            flush=True,
        )
    return 0


def uninstall(*, user_mode: bool, dry_run: bool) -> int:
    plan = resolve_plan(user_mode)
    if _system_mode_needs_root(plan):
        _print_root_required()
        return 1
    if systemctl_available():
        _run([*plan.systemctl_args, "disable", "--now", SERVICE_NAME], dry_run=dry_run)
    removed = _remove_unit(plan, dry_run=dry_run)
    if systemctl_available():
        _run([*plan.systemctl_args, "daemon-reload"], dry_run=dry_run)
    if dry_run:
        print(f"[dry-run] would remove {plan.unit_path}.", flush=True)
    elif removed:
        print(f"Removed {plan.unit_path}.", flush=True)
    else:
        print(f"No unit at {plan.unit_path}; nothing to remove.", flush=True)
    return 0


def _systemctl_passthrough(plan: UnitPlan, verb: str, *, dry_run: bool) -> int:
    """Run `systemctl <verb> awsqe-host`; let stdout/stderr stream through."""
    if dry_run:
        argv = [*plan.systemctl_args, verb, SERVICE_NAME]
        print(f"[dry-run] {' '.join(argv)}", flush=True)
        return 0
    result = subprocess.run([*plan.systemctl_args, verb, SERVICE_NAME])
    return result.returncode


def start(*, user_mode: bool, dry_run: bool) -> int:
    """Start the daemon via systemctl; fall back to foreground if systemd is absent."""
    plan = resolve_plan(user_mode)
    if systemctl_available():
        return _systemctl_passthrough(plan, "start", dry_run=dry_run)
    print(
        "systemctl not available; falling back to foreground `awsqe-host monitor`. "
        "Press Ctrl-C to stop.",
        flush=True,
        file=sys.stderr,
    )
    if dry_run:
        print("[dry-run] awsqe-host monitor", flush=True)
        return 0
    from .cli import cmd_monitor

    class _Args:
        hosts_file = None

    cmd_monitor(_Args())
    return 0


def stop(*, user_mode: bool, dry_run: bool) -> int:
    plan = resolve_plan(user_mode)
    if systemctl_available():
        return _systemctl_passthrough(plan, "stop", dry_run=dry_run)
    # Fallback: use the existing pidfile-based stop path.
    if dry_run:
        print("[dry-run] (no systemctl) signal pidfile-tracked monitor", flush=True)
        return 0
    from .cli import cmd_stop_monitor

    return cmd_stop_monitor(None)


def restart(*, user_mode: bool, dry_run: bool) -> int:
    plan = resolve_plan(user_mode)
    if not systemctl_available():
        print(
            "systemctl not available; restart is only meaningful under systemd. "
            "Stop the foreground process and re-run `awsqe-host start`.",
            flush=True,
            file=sys.stderr,
        )
        return 1
    return _systemctl_passthrough(plan, "restart", dry_run=dry_run)


def status(*, user_mode: bool, dry_run: bool) -> int:
    plan = resolve_plan(user_mode)
    if systemctl_available():
        return _systemctl_passthrough(plan, "status", dry_run=dry_run)
    # Fallback: report pidfile state.
    if dry_run:
        print("[dry-run] (no systemctl) check pidfile", flush=True)
        return 0
    from .cli import cmd_status_monitor

    return cmd_status_monitor(None)


def logs(*, user_mode: bool, follow: bool, lines: int | None, dry_run: bool) -> int:
    plan = resolve_plan(user_mode)
    if not shutil.which("journalctl"):
        print(
            "journalctl not available. If you are running the monitor in the "
            "foreground, watch its stdout/stderr directly.",
            flush=True,
            file=sys.stderr,
        )
        return 1
    argv = [*plan.journalctl_args, "-u", SERVICE_NAME]
    if follow:
        argv.append("-f")
    if lines is not None:
        argv.extend(["-n", str(lines)])
    if dry_run:
        print(f"[dry-run] {' '.join(argv)}", flush=True)
        return 0
    try:
        return subprocess.run(argv).returncode
    except KeyboardInterrupt:
        # `awsqe-host logs -f` blocks indefinitely; Ctrl-C is the documented
        # way to exit. Swallow it so the user gets a clean shell prompt back
        # instead of a multi-line traceback. SIGINT exit code is 128+2=130.
        return 130
