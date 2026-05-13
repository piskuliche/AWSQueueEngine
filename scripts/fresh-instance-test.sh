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
echo "--- 2. running install + lifecycle test on remote ---"
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

section "install package (--user, editable)"
python3 -m pip install --user -e . 2>&1 | tail -6
# Make sure pip user bin dir is on PATH for this shell.
export PATH="$HOME/.local/bin:$PATH"
echo "awsqe-host -> $(command -v awsqe-host || echo NOT FOUND)"

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
sudo -n true 2>/dev/null \
    && sudo awsqe-host install --dry-run \
    || echo "(skipped: no passwordless sudo available on this host)"

section "uninstall user daemon"
awsqe-host uninstall --user

section "post-uninstall checks"
awsqe-host status --user 2>&1 | head -3 || true
ls ~/.config/systemd/user/awsqe-host* 2>/dev/null || echo "(no unit file left behind)"
ls ~/.config/systemd/user/default.target.wants/ 2>/dev/null || echo "(no enabled symlink left)"

section "clear sandbox env"
systemctl --user unset-environment AWSQUEUEENGINE_QUEUES
systemctl --user show-environment | grep AWSQUEUEENGINE_QUEUES || echo "(QUEUES env cleared)"

section "state files in remote ~/ "
# On a fresh instance these should be present (the daemon wrote them as it
# polled). Confirm they're not referencing any real eci-* hostname.
ls -la ~/.aws_slurm_like_* 2>/dev/null | head
echo "--- queue contents:"
cat ~/.aws_slurm_like_queue.json 2>/dev/null || echo "(no queue file)"
echo "--- running jobs:"
cat ~/.aws_slurm_like_running.json 2>/dev/null || echo "(no running file)"

section "done"
EOF

echo
echo "=== fresh-instance test complete on ${REMOTE_HOST} ==="
echo "Source tree is left at ~/${REMOTE_REPO} on the remote for further poking."
echo "To fully clean up: ssh ${REMOTE_HOST} 'rm -rf ~/AWSQueueEngine ~/.aws_slurm_like_*.json'"
