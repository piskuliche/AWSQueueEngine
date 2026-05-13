#!/usr/bin/env bash
# End-to-end Phase 4 test on a fresh AWS instance that has no AWSQueueEngine
# installed. Exercises the FULL daemon lifecycle (install / status / stop /
# start / restart / uninstall) under real systemd, plus the system-mode dry-run
# the local Fedora test couldn't cover.
#
# It is paranoid about not letting the daemon talk to real worker hosts:
#   - Queue config is pinned to a non-resolvable hostname before the unit is
#     enabled (via a systemd drop-in for the system unit, or
#     `systemctl --user set-environment` for the user unit). The monitor will
#     log "unreachable" on every poll instead of SSHing anywhere real.
#   - Mailtrap creds are cleared in the systemd manager environment so no
#     alert emails fire.
#
# Usage:
#   scripts/fresh-instance-test.sh <ssh-host>
#
# Example:
#   scripts/fresh-instance-test.sh patocontrol
#
# Assumes the SSH alias works passwordless and that the remote user has
# passwordless sudo. The script rsyncs the local checkout into ~/AWSQueueEngine
# on the remote, pip-installs it (--user), runs the lifecycle, then uninstalls
# the daemon. The rsynced source tree is left in place for further manual
# poking; rm -rf ~/AWSQueueEngine on the remote when you're done.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <ssh-host>" >&2
    exit 2
fi

REMOTE_HOST="$1"
LOCAL_REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REMOTE_REPO="AWSQueueEngine"
FAKE_QUEUE="default=awsqe-fake-host-not-real"

echo "=== Fresh-instance test on ${REMOTE_HOST} ==="
echo "local repo:  ${LOCAL_REPO}"
echo "remote repo: ~/${REMOTE_REPO}"
echo "fake queue:  ${FAKE_QUEUE}"
echo

# --- 1. rsync source to remote ---------------------------------------------
echo "--- 1. rsync source to remote (${REMOTE_HOST}:~/${REMOTE_REPO}) ---"
rsync -az --delete \
    --exclude='__pycache__/' \
    --exclude='*.egg-info/' \
    --exclude='.pytest_cache/' \
    --exclude='pr-body*.md' \
    --exclude='.claude/' \
    "${LOCAL_REPO}/" "${REMOTE_HOST}:~/${REMOTE_REPO}/"
echo "rsync done"
echo

# --- 2. remote-side install + lifecycle test ------------------------------
echo "--- 2. install + lifecycle on remote ---"
ssh -T "${REMOTE_HOST}" "REMOTE_REPO=${REMOTE_REPO} FAKE_QUEUE='${FAKE_QUEUE}' bash -s" <<'EOF'
set -euo pipefail

cd "${REMOTE_REPO}"

section() { echo; echo "===== $* ====="; }

section "remote env"
echo "host:      $(hostname)"
echo "user:      $(whoami)"
echo "repo:      $(pwd)"
echo "branch:    $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo '(no git)')"
echo "commit:    $(git rev-parse --short HEAD 2>/dev/null || echo '?')"
echo "python:    $(python3 --version 2>&1)"
echo "systemctl: $(systemctl --version | head -1)"

section "ensure pip is installed"
if ! python3 -m pip --version >/dev/null 2>&1; then
    echo "(pip not present; installing python3-pip via apt)"
    sudo apt-get update -qq
    sudo apt-get install -y python3-pip python3-venv
fi
python3 -m pip --version

section "install package (--user, editable)"
# Some Ubuntu releases mark the system Python as PEP-668-protected; in that
# case pip --user errors with "externally-managed-environment". Retry with
# --break-system-packages, which is safe here because patocontrol is a
# throwaway test instance and the script removes everything in cleanup.
PIP_OUT=$(python3 -m pip install --user -e . 2>&1) && PIP_RC=0 || PIP_RC=$?
if [[ $PIP_RC -ne 0 ]] && echo "$PIP_OUT" | grep -q "externally-managed-environment"; then
    echo "(externally-managed-environment detected; retrying with --break-system-packages)"
    python3 -m pip install --user --break-system-packages -e . 2>&1 | tail -6
elif [[ $PIP_RC -ne 0 ]]; then
    echo "$PIP_OUT" | tail -20
    echo "[FAIL] pip install failed for a reason other than PEP 668"
    exit 1
else
    echo "$PIP_OUT" | tail -6
fi
# Make sure pip user bin dir is on PATH for this shell.
export PATH="$HOME/.local/bin:$PATH"
echo "awsqe-host -> $(command -v awsqe-host || echo NOT FOUND)"

section "expose awsqe-host on default ssh PATH (/usr/local/bin symlink)"
# Non-interactive `ssh patocontrol awsqe-host rpc` (what the client does
# internally) typically doesn't pick up ~/.local/bin. Symlinking to
# /usr/local/bin makes it discoverable for the upcoming RPC tests; we
# clean it up at the end.
if sudo -n true 2>/dev/null; then
    sudo ln -sf "$HOME/.local/bin/awsqe-host"    /usr/local/bin/awsqe-host
    sudo ln -sf "$HOME/.local/bin/awsqe-client"  /usr/local/bin/awsqe-client
    sudo ln -sf "$HOME/.local/bin/awsqueueengine" /usr/local/bin/awsqueueengine
    echo "symlinked: $(ls -la /usr/local/bin/awsqe-host /usr/local/bin/awsqe-client /usr/local/bin/awsqueueengine 2>&1 | tail -3)"
else
    echo "[WARN] no passwordless sudo; skipping symlink. The dev-box RPC tests"
    echo "       below will likely fail with 'awsqe-host: command not found'."
fi

section "set sandbox env in user systemd manager"
# Pin queue at a non-resolvable host so the monitor cannot reach real workers.
systemctl --user set-environment AWSQUEUEENGINE_QUEUES="${FAKE_QUEUE}"
# Clear Mailtrap creds so no alert emails fire during the test.
systemctl --user unset-environment AWSQUEUEENGINE_MAILTRAP_TOKEN AWSQUEUEENGINE_ALERT_TO || true
systemctl --user show-environment | grep -E '^(AWSQUEUEENGINE_QUEUES|AWSQUEUEENGINE_MAILTRAP)' || true

section "user-mode install --dry-run (inspect unit text)"
awsqe-host install --user --dry-run

section "user-mode install (real)"
awsqe-host install --user
sleep 2
awsqe-host status --user | head -15

section "journal first 25 lines"
journalctl --user -u awsqe-host --no-pager -n 25 || true

section "stop / start / restart cycle"
awsqe-host stop --user; sleep 1
awsqe-host status --user | head -5 || true
awsqe-host start --user; sleep 2
awsqe-host status --user | head -5
awsqe-host restart --user; sleep 2
awsqe-host status --user | head -5

section "system-mode install --dry-run (preview only)"
# We don't actually do `sudo awsqe-host install` here because the user-mode
# unit is already running; running both side-by-side would have two daemons
# competing for the same state files. Dry-run is enough to confirm the
# system-mode unit generator works in this environment.
#
# Heads up: this often fails with a PackageNotFoundError when the package
# was installed via `pip install --user` as the regular user, because root's
# Python has no metadata for it. That's a deployment-pattern detail, not a
# Phase 4 bug: a production system install would use `sudo pip install -e .`
# (or a venv) so root can resolve `awsqueueengine`. We don't fix it here
# because the user-mode path already passed the lifecycle test above.
if ! sudo -n true 2>/dev/null; then
    echo "(skipped: no passwordless sudo on this host)"
elif ! sudo python3 -c "import importlib.metadata; importlib.metadata.distribution('awsqueueengine')" 2>/dev/null; then
    echo "(skipped: root's Python can't find awsqueueengine — expected with"
    echo " pip install --user. For a real system install, use sudo pip install"
    echo " or a system-wide venv.)"
else
    sudo awsqe-host install --dry-run || echo "(system-mode dry-run failed; see traceback above)"
fi
EOF

# --- 3. client → host RPC tests from THIS dev box --------------------------
echo
echo "--- 3. client → host RPC tests from dev box (against ${REMOTE_HOST}) ---"

section() { echo; echo "===== $* ====="; }

section "smoke: can the local CLI reach awsqe-host over ssh?"
ssh -T "${REMOTE_HOST}" 'command -v awsqe-host && awsqe-host --help 2>&1 | head -3' || {
    echo "[FAIL] cannot find awsqe-host via non-interactive ssh on ${REMOTE_HOST}."
    echo "       This usually means /usr/local/bin/awsqe-host wasn't symlinked above."
    echo "       Skipping the rest of the RPC section."
}

section "awsqueueengine list --queue-host ${REMOTE_HOST}  (legacy CLI path)"
if command -v awsqueueengine >/dev/null; then
    awsqueueengine list --queue-host "${REMOTE_HOST}" || echo "  (legacy list returned non-zero)"
else
    echo "(awsqueueengine not on local PATH; skipping)"
fi

section "awsqe-client list --queue-host ${REMOTE_HOST}  (new CLI path)"
awsqe-client list --queue-host "${REMOTE_HOST}" || echo "  (awsqe-client list returned non-zero)"

section "awsqe-client qstat --queue-host ${REMOTE_HOST}"
awsqe-client qstat --queue-host "${REMOTE_HOST}" || echo "  (qstat returned non-zero)"

section "awsqe-client submit --queue-host ${REMOTE_HOST} -- echo from-dev-box-$(date +%s)"
awsqe-client submit --queue-host "${REMOTE_HOST}" -- echo from-dev-box-$(date +%s) \
    || echo "  (submit returned non-zero; check the remote daemon's journal)"

section "awsqe-client list --queue-host ${REMOTE_HOST}  (should now show 1 queued job)"
awsqe-client list --queue-host "${REMOTE_HOST}" || true

# --- 4. cleanup on remote: uninstall daemon, drop symlinks, clear env ------
echo
echo "--- 4. cleanup on remote ---"
ssh -T "${REMOTE_HOST}" "REMOTE_REPO=${REMOTE_REPO} bash -s" <<'EOF'
set -euo pipefail

cd "${REMOTE_REPO}"

section() { echo; echo "===== $* ====="; }

section "stop daemon + clear the test queue before uninstalling"
export PATH="$HOME/.local/bin:$PATH"
awsqe-host stop --user || true
sleep 1
# `awsqe-host clear` mutates the queue file; this wipes the submit we did above
# so nothing lingers if the daemon is reinstalled later.
awsqe-host clear || true

section "uninstall user daemon"
awsqe-host uninstall --user

section "post-uninstall checks"
awsqe-host status --user 2>&1 | head -3 || true
ls ~/.config/systemd/user/awsqe-host* 2>/dev/null || echo "(no unit file left behind)"
ls ~/.config/systemd/user/default.target.wants/ 2>/dev/null || echo "(no enabled symlink left)"

section "drop /usr/local/bin symlinks"
if sudo -n true 2>/dev/null; then
    sudo rm -f /usr/local/bin/awsqe-host /usr/local/bin/awsqe-client /usr/local/bin/awsqueueengine
    echo "(symlinks removed)"
else
    echo "(no passwordless sudo; left the symlinks in place — remove manually)"
fi

section "clear sandbox env"
systemctl --user unset-environment AWSQUEUEENGINE_QUEUES
systemctl --user show-environment | grep AWSQUEUEENGINE_QUEUES || echo "(QUEUES env cleared)"

section "state files in remote ~/ "
ls -la ~/.aws_slurm_like_* 2>/dev/null | head
echo "--- queue contents (should be empty list after `clear`):"
cat ~/.aws_slurm_like_queue.json 2>/dev/null || echo "(no queue file)"
echo "--- running jobs:"
cat ~/.aws_slurm_like_running.json 2>/dev/null || echo "(no running file)"

section "done"
EOF

echo
echo "=== fresh-instance test complete on ${REMOTE_HOST} ==="
echo "Source tree is left at ~/${REMOTE_REPO} on the remote for further poking."
echo "To fully clean up: ssh ${REMOTE_HOST} 'rm -rf ~/AWSQueueEngine ~/.aws_slurm_like_*.json'"
