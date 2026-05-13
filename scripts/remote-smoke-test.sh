#!/usr/bin/env bash
# Remote smoke test for Phase 4: validates the daemon code on a queue host
# WITHOUT touching the production queue state, workers, or alert pipeline.
#
# Two checks run on the remote:
#
#   1. `awsqe-host install --user --dry-run`
#      — Verifies the unit-file generator works in the remote environment
#        (resolves $SUDO_USER, picks the right ExecStart path, etc.). Writes
#        nothing, runs no systemctl commands.
#
#   2. Foreground monitor for ~8 seconds with a sandboxed HOME and a fake
#      queue host
#      — Runs the same code systemd's ExecStart would run, but with:
#         * HOME pointed at a tempdir (so state files like
#           ~/.aws_slurm_like_queue.json are read/written under /tmp, not
#           your production home),
#         * AWSQUEUEENGINE_QUEUES pointed at a non-resolvable hostname (so
#           the monitor's SSH polls fail cleanly instead of touching real
#           workers),
#         * Mailtrap creds cleared (no alert emails fire during the test).
#      timeout(1) sends SIGTERM after 8 seconds; the monitor exits cleanly.
#
# What this DOESN'T test:
#   - The actual `systemctl enable --now` mechanics on AWS Ubuntu. That's
#     left to a real cutover during a maintenance window.
#
# Usage:
#   scripts/remote-smoke-test.sh <ssh-host> [repo-path-on-remote]
#
# Examples:
#   scripts/remote-smoke-test.sh queue-manager
#   scripts/remote-smoke-test.sh queue-manager /home/ubuntu/AWSQueueEngine

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <ssh-host> [repo-path-on-remote]" >&2
    exit 2
fi

REMOTE_HOST="$1"
REMOTE_REPO="${2:-AWSQueueEngine}"

echo "=== Remote smoke test on ${REMOTE_HOST} (repo: ${REMOTE_REPO}) ==="
echo

# The heredoc body runs on the remote. Single-quoted 'EOF' so locals don't
# expand here; we pass REMOTE_REPO in via the shell prelude.
ssh -T "${REMOTE_HOST}" "REMOTE_REPO=${REMOTE_REPO} bash -s" <<'EOF'
set -euo pipefail

cd "${REMOTE_REPO}" || { echo "no such repo on remote: ${REMOTE_REPO}" >&2; exit 1; }

echo "--- remote env ---"
echo "host:      $(hostname)"
echo "user:      $(whoami)"
echo "repo:      $(pwd)"
echo "branch:    $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo '(not a git repo)')"
echo "commit:    $(git rev-parse --short HEAD 2>/dev/null || echo '?')"
echo "python:    $(python --version 2>&1)"
echo "systemctl: $(systemctl --version 2>/dev/null | head -1 || echo 'NOT INSTALLED')"
echo

# Warn if not on a phase4-* branch — the smoke test only makes sense for
# Phase 4 code.
BRANCH="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
case "$BRANCH" in
    phase4-*|create_client)
        ;;
    *)
        echo "[WARN] remote checkout is on '$BRANCH', not phase4-daemon / create_client."
        echo "       Proceeding anyway, but `awsqe-host` may not exist in this checkout."
        echo
        ;;
esac

echo "--- 1. awsqe-host install --user --dry-run ---"
PYTHONPATH=src python -m awsqueueengine.host.cli install --user --dry-run
echo

echo "--- 2. sandboxed foreground monitor (8s; sandbox=/tmp/awsqe-smoke.*) ---"
SANDBOX=$(mktemp -d /tmp/awsqe-smoke.XXXXXX)
echo "sandbox HOME: $SANDBOX"
echo "queue:        AWSQUEUEENGINE_QUEUES=default=awsqe-fake-does-not-resolve"
echo "mailtrap:     cleared"
echo

set +e
HOME="$SANDBOX" \
AWSQUEUEENGINE_QUEUES="default=awsqe-fake-does-not-resolve" \
AWSQUEUEENGINE_MAILTRAP_TOKEN= \
AWSQUEUEENGINE_ALERT_TO= \
PYTHONPATH=src \
    timeout --signal=SIGTERM 8 python -m awsqueueengine.host.cli monitor 2>&1
RC=$?
set -e
echo
echo "monitor rc=$RC (124 = timeout killed it after grace; 143 = exited via SIGTERM; 0 = exited clean)"
echo

echo "--- 3. state files created under the sandbox ---"
if ls -la "$SANDBOX"/.aws_slurm_like_* 2>/dev/null; then
    echo "  (these are sandbox-only; the prod ~/.aws_slurm_like_*.json was NOT touched)"
else
    echo "  (none — expected when the queue stays empty)"
fi

echo
echo "--- 4. cleanup ---"
rm -rf "$SANDBOX"
echo "removed $SANDBOX"
echo
echo "--- 5. confirm prod state files are untouched ---"
ls -la ~/.aws_slurm_like_*.json 2>/dev/null | head -5 || echo "  (no prod state files in home — interesting; check if daemon runs elsewhere)"
EOF

echo
echo "=== smoke test complete ==="
echo "If you see exit codes 0 / 124 / 143 above and the prod state-file mtimes are"
echo "unchanged from before the run, Phase 4 is operationally green on this host."
