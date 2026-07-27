# AWSQueueEngine

A Slurm-like SSH job manager for AWS GPU hosts, with payload staging and monitoring.

The codebase is split into two binaries that talk to each other over a
JSON-over-SSH RPC:

- **`awsqe-client`** — runs on your laptop / dev box; archives payloads,
  uploads them to S3, and asks the queue host to enqueue jobs.
- **`awsqe-host`** — runs on the queue-manager VM as a systemd service;
  owns the queue state, dispatches jobs to worker hosts over SSH, and
  sends email alerts.

The legacy `awsqueueengine` command remains as a backward-compat shim that
dispatches to one of the two CLIs based on what subcommand you ran.

Requires **Python 3.10+** on both the local submitter and the queue host.

## Installation

### Local submitter (laptop / dev box)

A `pip install --user` is fine here. On Ubuntu 23.04+ you may need the
PEP 668 escape hatch:

```bash
git clone <repo-url> AWSQueueEngine
cd AWSQueueEngine
python3 -m pip install --user -e .                          # most systems
# or, on Ubuntu 23.04+:
python3 -m pip install --user --break-system-packages -e .
```

`awsqe-client`, `awsqe-host`, and `awsqueueengine` all land in
`~/.local/bin`. Make sure that's on `$PATH`.

### Queue host (production)

Use a **dedicated venv** so the daemon's dependencies don't fight Debian-
or apt-installed system packages (PEP 668), and **symlink the binaries
into `/usr/local/bin`** so non-interactive SSH from clients can find
`awsqe-host` on PATH:

```bash
sudo apt-get install -y python3 python3-venv python3-pip git

git clone <repo-url> ~/AWSQueueEngine
cd ~/AWSQueueEngine

sudo python3 -m venv /opt/awsqueueengine-venv
sudo /opt/awsqueueengine-venv/bin/pip install -U pip
sudo /opt/awsqueueengine-venv/bin/pip install -e .

sudo ln -sf /opt/awsqueueengine-venv/bin/awsqe-host    /usr/local/bin/awsqe-host
sudo ln -sf /opt/awsqueueengine-venv/bin/awsqe-client  /usr/local/bin/awsqe-client
sudo ln -sf /opt/awsqueueengine-venv/bin/awsqueueengine /usr/local/bin/awsqueueengine
```

Verify:

```bash
which awsqe-host && head -1 $(which awsqe-host)
# Should show /usr/local/bin/awsqe-host with a shebang pointing at
# /opt/awsqueueengine-venv/bin/python3
```

Cleanup later, if needed: `sudo rm -rf /opt/awsqueueengine-venv /usr/local/bin/{awsqe-host,awsqe-client,awsqueueengine}`.

## Client configuration

Configure the client once so you don't have to pass `--queue-host` or S3
flags on every command. Settings live in `~/.awsqe/client/config.toml`:

```bash
awsqe-client config set queue-host queue-manager
awsqe-client config set s3.bucket   amberflow-default
awsqe-client config set s3.prefix   jobs   
awsqe-client config show                                       # inspect what's set
awsqe-client config unset queue-host                           # clear one key
```

Resolution precedence per setting is **CLI flag > env var > config > error**.
The legacy `awsqueueengine` CLI reads the same config, so `awsqueueengine list`
with no flag routes to the configured queue host. To force a local read on
the queue host itself, use `awsqe-host list` directly.

For S3-backed payload submit you also need AWS credentials with write
access to the bucket configured locally (the usual `~/.aws/credentials`,
env vars, or IAM role).

## Usage

Most days you'll only need these:

```bash
# Submit a job to the configured queue host:
awsqe-client submit --payload ./my_payload "cd $PAYLOAD_DIR && bash run.sh"
awsqe-client submit --queue fast-gpus "python train.py"
awsqe-client submit --priority 25 "python train.py --epochs 10"
awsqe-client submit --preempt --priority 999 "bash urgent-job.sh"

# Inspect the queue host:
awsqe-client list                  # queued jobs
awsqe-client qstat                 # running jobs (elapsed HH:MM:SS each)
awsqe-client failed                # jobs that failed, newest first, with a reason
awsqe-client failed --job-id JOB --log   # plus the captured tail of that job's log
awsqe-client deferred              # jobs that exceeded the submit-failure limit
awsqe-client requeue-deferred --all
awsqe-client enable-host           # show active host cooldowns (no args)
awsqe-client enable-host eci17     # release a host from cooldown early

# Worker-host operations from your laptop (SSHes to the worker directly):
awsqe-client status                # ps probe of every host's MANAGER_TAG state
awsqe-client tail eci17            # tail the most recent job log on a worker
awsqe-client stop eci17            # kill managed job(s) on a worker
awsqe-client where                 # probe scratch space on every worker
awsqe-client info -p ./my_payload  # refresh run.info from queue-host state

# Override config on a one-off command:
awsqe-client submit --queue-host other-queue "python sweep.py"
```

The legacy `awsqueueengine <subcommand>` still works for the commands it
shipped with (everything above except `failed`). It will be deprecated in
a later release; new scripts should use `awsqe-client`.

`submit --high-priority` is still supported for backward compatibility and
maps to priority `100`. If both `--priority` and `--high-priority` are
supplied, `--priority` takes precedence.

`--queue <name>` submits to a user queue. Jobs store the queue name, and
the monitor assigns the concrete worker host at dispatch time from the
current queue config. `--hosts` and `--host-set` remain for legacy
scripts, but new submissions should prefer queues.

`--preempt` allows a queued job to interrupt a currently running managed
job when no free eligible host is available. The interrupted job is
requeued and restarted.

`requeue-running` kills monitor-tracked running job(s) and requeues them
back to their original host with priority `100`, preserving the remote
payload path.

`qstat` lists monitor-tracked running jobs and elapsed runtime
(`HH:MM:SS`). When jobs finish, the monitor appends completion records to
`~/.awsqe/host/completed.json` with the `qstat` fields plus final duration
and timestamps (`started_at`, `finished_at`).

### Failed jobs

Every job is launched wrapped so the worker writes its exit status to
`~/manager_jobs/<job_id>.rc` next to the job log. When the monitor sees a
host go idle it reads that status back: a clean `0` becomes a completion
record, and anything else becomes a **failure record** in
`~/.awsqe/host/failed.json` — including jobs that die seconds after
launch, which previously left no trace at all.

Each failure record carries the usual job fields plus `exit_code`, a
short `failure_reason` slug, a one-line `failure_detail`, and the last 40
lines of the job log (`log_tail`):

```bash
awsqe-client failed                      # 50 most recent, newest first
awsqe-client failed -n 200               # more history
awsqe-client failed --job-id JOB --log   # one job, with its captured log tail
awsqe-host failed --log                  # same view, on the queue host
```

`awsqe-client info -p ./my_payload` reports `status: failed` for a failed
job and writes `failure_reason` / `failure_detail` / `exit_code` into the
payload's `run.info`.

Reasons are a rough classification, taken from the log tail first and the
exit status second: `out_of_memory`, `disk_full`, `cuda_error`,
`command_not_found`, `permission_denied`, `python_import_error`,
`python_exception`, `not_executable`, `killed`, `segfault`,
`terminated`, `signal_N`, `nonzero_exit`, plus two that describe how the
job ended rather than why:

- `start_failed` — the job never ran (submit failure, or it exited before
  the monitor could confirm the process). Repeated start failures still
  move the job to `deferred.json` for requeueing as before.
- `no_exit_status` — the job vanished without recording a status. Usually
  a hard kill, a host reboot, or an operator running `stop` /
  `requeue-running` against the host; the failure history records those
  too rather than counting them as clean finishes.

The failure history is capped at the 1000 most recent records.

## Remote queue host setup

Once installed per the queue-host instructions above, define your worker
queues in a JSON file:

```bash
cat > /home/ubuntu/awsqueueengine_queues.json <<'JSON'
{
  "default":  ["eci1", "eci2", "eci3"],
  "fast-gpus": ["eci1", "eci2"],
  "large-mem": ["eci3"]
}
JSON
```

The queue config is the single source of truth for worker assignment.
Edit this file at any time — the monitor reloads it once per poll cycle
(~60s), no daemon restart needed. The journal will print
`[INFO] Queue hosts updated from ...` when a change is picked up.

For simple static setups you can skip the file and use one env var
instead: `AWSQUEUEENGINE_QUEUES="default=eci1,eci2;fast-gpus=eci3"`.
`AWSQUEUEENGINE_QUEUES_FILE` and `AWSQUEUEENGINE_QUEUES` are mutually
exclusive.

### Install the systemd service

```bash
# System-wide unit (requires sudo). Writes /etc/systemd/system/awsqe-host.service,
# daemon-reloads, and enables --now. Runs as $SUDO_USER so the daemon owns
# the same ~/.awsqe/host/ state files you migrated.
sudo awsqe-host install
sudo awsqe-host status
sudo awsqe-host logs -f       # system journal needs sudo or systemd-journal
                              # group membership
```

Per-user variant if you can't or don't want to use sudo:

```bash
awsqe-host install --user
loginctl enable-linger $USER  # so the daemon survives logout
awsqe-host logs --user -f
```

Other daemon verbs: `start | stop | restart | status | logs | uninstall`.
All accept `--user` and `--dry-run`. If systemd isn't available,
`awsqe-host start` falls back to a foreground run that you can Ctrl-C.

Legacy `awsqueueengine start-monitor` still works for backward
compatibility (foreground + pidfile) and is removed in a later release.

### Wiring config into the systemd unit

The systemd service starts with a **clean environment** — it does NOT
read your `~/.bashrc`. Tell it which queue config and Mailtrap creds to
use via a drop-in at `/etc/systemd/system/awsqe-host.service.d/override.conf`.

```bash
sudo tee /etc/systemd/system/awsqe-host.service.d/override.conf >/dev/null <<'EOF'
[Service]
Environment="AWSQUEUEENGINE_QUEUES_FILE=/home/ubuntu/awsqueueengine_queues.json"
Environment="AWSQUEUEENGINE_MAILTRAP_TOKEN=<your-mailtrap-token>"
Environment="AWSQUEUEENGINE_MAILTRAP_SENDER_EMAIL=hello@piskulich.com"
Environment="AWSQUEUEENGINE_MAILTRAP_SENDER_NAME=AWSQueueEngine"
Environment="AWSQUEUEENGINE_MAILTRAP_CATEGORY=Queue Monitor"
Environment="AWSQUEUEENGINE_ALERT_TO=you@example.com,team@example.com"
EOF

sudo systemctl daemon-reload
sudo systemctl restart awsqe-host
sudo systemctl show awsqe-host -p Environment
```

**Quote every `KEY=VALUE`** — systemd splits unquoted values on whitespace
and silently drops everything after the first space. (`Queue Monitor`
without quotes parses as `Queue` plus a junk `Monitor` token that systemd
warns about and discards.)

If you'd rather not have the Mailtrap token world-readable, put the
secret-bearing vars in a separate root:root mode-600 file and reference
it from the unit:

```bash
sudo tee /etc/awsqe-host.env >/dev/null <<'EOF'
AWSQUEUEENGINE_MAILTRAP_TOKEN=<token>
AWSQUEUEENGINE_MAILTRAP_SENDER_EMAIL=hello@piskulich.com
AWSQUEUEENGINE_ALERT_TO=you@example.com,team@example.com
EOF
sudo chmod 600 /etc/awsqe-host.env

# Then in /etc/systemd/system/awsqe-host.service.d/override.conf:
#   [Service]
#   Environment="AWSQUEUEENGINE_QUEUES_FILE=/home/ubuntu/awsqueueengine_queues.json"
#   EnvironmentFile=/etc/awsqe-host.env
```

### State migration (one-shot, from Phase 5 onward)

The queue host's state files moved from `~/.aws_slurm_like_*.json` to
`~/.awsqe/host/`:

```
~/.awsqe/host/queue.json
~/.awsqe/host/running.json
~/.awsqe/host/completed.json
~/.awsqe/host/failed.json      # no legacy counterpart; created on first failure
~/.awsqe/host/deferred.json
~/.awsqe/host/monitor_state.json
~/.awsqe/host/lock
~/.awsqe/host/pid
```

The daemon migrates them on first start. You can also run it explicitly:

```bash
awsqe-host migrate --dry-run     # preview what would move
awsqe-host migrate               # actually move (idempotent)
awsqe-host migrate --force       # re-run even if already migrated
```

For each legacy file the migration:
1. Copies it (preserving mtime/perms) to its new home in `~/.awsqe/host/`.
2. Renames the legacy file to `~/.aws_slurm_like_*.json.migrated.bak`.
3. Stamps `migrated_at` in the new `monitor_state.json` so subsequent
   runs are a no-op.

If you need to roll back, move the `.migrated.bak` files back to their
original names and remove the new `~/.awsqe/host/` directory:

```bash
for f in ~/.aws_slurm_like_*.migrated.bak; do mv "$f" "${f%.migrated.bak}"; done
rm -rf ~/.awsqe/host
```

## Worker host setup

Each worker host must be reachable by SSH from the queue host and have
scratch space under the configured scratch roots. For S3-backed payloads,
workers also need the AWS CLI and IAM permissions to read the payload
bucket/prefix:

```bash
aws s3 ls s3://my-queue-payload-bucket/awsqueueengine/payloads/
```

## Remote submit with S3 payloads

`awsqe-client submit --payload <dir>` archives the directory locally,
uploads it to S3, then asks the queue host to enqueue a job that
references the S3 URI. The monitor on the queue host later asks the
selected worker to download and extract that archive before running the
job.

Submitter requirements (one-time setup):

```bash
awsqe-client config set s3.bucket  my-queue-payload-bucket
awsqe-client config set s3.prefix  awsqueueengine/payloads   # optional

# Then:
awsqe-client submit --payload ./my_payload "cd $PAYLOAD_DIR && bash run.sh"
```

The submitter needs AWS credentials with write access to the
bucket/prefix. Worker hosts need read access to the same prefix. The
queue host validates the queue name against
`AWSQUEUEENGINE_QUEUES_FILE` or `AWSQUEUEENGINE_QUEUES` configured on it.
Configure an S3 lifecycle rule for the payload prefix to clean up old
archives.

`AWSQUEUEENGINE_S3_BUCKET` and `AWSQUEUEENGINE_S3_PREFIX` env vars still
work and override the config when set, mostly useful for one-off
overrides in CI.

## Email Alerts (Mailtrap API)

Configure the Mailtrap creds via the systemd unit drop-in shown above
(see "Wiring config into the systemd unit"). The relevant vars:

```
AWSQUEUEENGINE_MAILTRAP_TOKEN              # required for any email to send
AWSQUEUEENGINE_MAILTRAP_SENDER_EMAIL       # required; the From address
AWSQUEUEENGINE_MAILTRAP_SENDER_NAME        # optional; display name in From header
AWSQUEUEENGINE_MAILTRAP_CATEGORY           # optional; tags emails on the Mailtrap side
AWSQUEUEENGINE_ALERT_TO                    # comma-separated recipients
AWSQUEUEENGINE_ALERT_DAILY_EMAIL_LIMIT     # default 150
AWSQUEUEENGINE_JOB_FAIL_ALERT_COOLDOWN_SECONDS  # default 900
```

When configured, the monitor sends email:

1. When a queued job fails to start on a host.
2. Once when queue depth drops below 10 (fires on transition into low-queue state).
3. Once when queue depth reaches 0 (fires on transition into empty state).
4. Once per calendar day when the monitor detects a new date, with a status summary.
5. When a host gets placed in cooldown (storage or transport failure).

Email rate limits:
- Total outgoing emails are capped per day (`AWSQUEUEENGINE_ALERT_DAILY_EMAIL_LIMIT`).
- Job-failure emails are rate-limited with a cooldown (`AWSQUEUEENGINE_JOB_FAIL_ALERT_COOLDOWN_SECONDS`).

To smoke-test the credentials from the queue host:

```bash
awsqe-host --test-email-connection
```

(This reads creds from the calling shell's environment, not from the
systemd unit. To verify the systemd unit's view, look at
`sudo systemctl show awsqe-host -p Environment` and let the daemon's
next alert-eligible event actually fire.)

## Project Structure

- `src/awsqueueengine/shared/` — data models, protocol/RPC, paths used by both sides
- `src/awsqueueengine/client/` — `awsqe-client` CLI, submit, run.info, RPC transport
- `src/awsqueueengine/host/` — `awsqe-host` CLI, monitor, job control, migration, daemon
- `src/awsqueueengine/cli.py` — legacy `awsqueueengine` shim that dispatches to one of the two
- `scripts/` — local-only utilities (smoke tests for remote queue host validation)
- `tests/` — unit + subprocess tests
- `setup.py` — packaging
