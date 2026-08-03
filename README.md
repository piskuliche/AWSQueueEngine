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

# Track your own submissions (a local list; works without a payload dir):
awsqe-client jobs                        # everything this machine submitted, newest first
awsqe-client jobs --status active        # still submitted, queued or running
awsqe-client jobs --status failed --since 7d
awsqe-client jobs --queue zeke-queue     # one queue (repeatable, comma-separated)
awsqe-client submit --payload-glob 'IDC*' "cd \$PAYLOAD_DIR && python run.py"
awsqe-client submit --payload-glob 'IDC*' --dry-run "..."   # list, submit nothing
awsqe-client jobs --array ffpopt-IDC     # list one batch's jobs individually
awsqe-client jobs --expand               # never collapse batches into one row
awsqe-client jobs --no-refresh           # skip the queue-host round trip
awsqe-client jobs --fetch-logs           # pull each shown job's log off its worker
awsqe-client jobs --cat 20260730-1415    # print one job's log to the screen
awsqe-client jobs --log 20260730-1415    # print the local path to one job's log
awsqe-client jobs --forget 20260730-1415 # stop tracking (does NOT cancel)
awsqe-client info --job-id 20260730-1415 # refresh one job without a payload dir

# Delete queued jobs, addressed by the job id `list` prints as [job=...]:
awsqe-client qdel 20260730-141530-a1b2c3
awsqe-client qdel 20260730-141530-a1b2c3 20260730-141602-9f0e11   # several at once
awsqe-client qdel 20260730-1415    # any unique prefix of a job id
awsqe-client qdel --queue fast-gpus   # every queued job in one queue
awsqe-client qdel --array ffpopt-IDC  # every queued job in one batch

# Group the host views by batch too (opt-in; these are everyone's jobs):
awsqe-client list --group
awsqe-client qstat --group

# Worker-host operations from your laptop (SSHes to the worker directly):
awsqe-client status                # ps probe of every host's MANAGER_TAG state
awsqe-client tail eci17            # tail the most recent job log on a worker
awsqe-client stop eci17            # kill managed job(s) on a worker
awsqe-client where                 # probe scratch space on every worker
awsqe-client info -p ./my_payload  # refresh run.info from queue-host state

# Override config on a one-off command:
awsqe-client submit --queue-host other-queue "python sweep.py"
```

`qdel` selects by job id rather than by the number `list` prints in the left
column. That number is only a render-time position and it moves constantly:
every deletion renumbers the entries after it, the monitor dequeues the
highest-priority job from anywhere in the list, requeues insert at the front,
and other users' submits append. Deleting several jobs from one listing by
position therefore hits the wrong ones. `--index N` still deletes by position
for the cases where that's genuinely what you want, and the three selectors
(job ids, `--index`, `--queue`) cannot be combined in one command. Nothing is
removed unless every selector resolves, so a typo leaves the queue untouched.

The legacy `awsqueueengine <subcommand>` still works for the commands it
shipped with (everything above except `failed` and `jobs`). It will be
deprecated in a later release; new scripts should use `awsqe-client`.

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

- `start_failed` — the job never ran (submit failure). Repeated start
  failures still move the job to `deferred.json` for requeueing as before.

A job that exits before the monitor can confirm its pid is judged by its
recorded status, not by its absence: exit `0` there means the work was
already done and the job is filed as **completed** (no alert, and the
host stays available for the next queued job), while a nonzero or missing
status is filed as a failure.
- `no_exit_status` — the job vanished without recording a status. Usually
  a hard kill, a host reboot, or an operator running `stop` /
  `requeue-running` against the host; the failure history records those
  too rather than counting them as clean finishes.

This only applies to jobs *this* monitor launched, which it marks with
`exit_status_tracked` in `running.json`. Jobs already running when the
daemon is upgraded never got the wrapper, so no status file exists for
them; those finish as `status: unknown` in `completed.json` (never as
failures) — a clean multi-hour run must not be reported as broken just
because its outcome can't be proven. The distinction is only relevant for
the jobs in flight across one upgrade.

The failure history is capped at the 1000 most recent records.

#### Your command's exit status is what gets recorded

The status written to `<job_id>.rc` is the **shell's** exit status, which with
`;`-chained commands is the status of the *last* one. A trailing cleanup step
therefore hides whatever went wrong before it:

```bash
# python fails (exit 1), rm succeeds (exit 0) -> the shell exits 0
# -> recorded as COMPLETED, no failure record, no alert
cd $PAYLOAD_DIR; python run_fe.py; rm -rf $PAYLOAD_DIR
```

Chain with `&&` instead. The status is then the first failure's, and the payload
directory survives for you to inspect:

```bash
cd $PAYLOAD_DIR && python run_fe.py && rm -rf $PAYLOAD_DIR
```

If cleanup has to happen either way, capture the status first:

```bash
cd $PAYLOAD_DIR; python run_fe.py; rc=$?; rm -rf $PAYLOAD_DIR; exit $rc
```

Nothing on the queue host can detect this — a shell that exits 0 finished
successfully as far as any caller can tell. `awsqe-client jobs --fetch-logs` is
the practical check: pull the logs and look for tracebacks in jobs that claim to
have completed.

### Tracked jobs

`list`, `qstat` and `failed` are **global** views: the queue host records no
notion of who submitted what, so it can't answer "how are *my* jobs doing?".
`awsqe-client jobs` answers that from a local ledger at
`~/.awsqe/client/jobs.json`, appended on every `awsqe-client submit` —
including payload-less ones, which write no `run.info` anywhere:

```
$ awsqe-client jobs --since 2d
SUBMITTED             JOB                     STATUS      HOST      QUEUE         DUR       CMD
2026-07-30 14:15:30   20260730-141530-a1b2c3  running     eci7      fast-gpus     -         python train.py
2026-07-30 09:02:11   20260730-090211-9f0e11  failed      eci3      default       01:44:20  bash run.sh
                      -> out_of_memory exit=137 (see `awsqe-client failed --job-id 20260730-090211-9f0e11 --log`)
2026-07-29 22:10:03   20260729-221003-4c5d6e  queued      -         default       q#3       python sweep.py
```

Each run refreshes every job that isn't already finished, in one round trip per
queue host, and writes the results back. `--no-refresh` skips the network
entirely. If a queue host is unreachable, the affected jobs keep their
last-known status, a warning goes to stderr, and the rest still refresh — the
list is local, so it stays useful offline.

The `DUR` column does double duty: elapsed time for finished jobs, queue
position (`q#3`) for queued ones. Running jobs show `-`, because the host
reports start times as preformatted text in its own timezone.

| Status | Meaning |
| --- | --- |
| `submitted` | enqueued; not yet refreshed from the queue host |
| `queued` | waiting on the queue host |
| `running` | dispatched to a worker |
| `completed` | finished with exit `0` |
| `failed` | finished badly; see `awsqe-client failed` |
| `unknown` | finished, but without a provable exit status (see above) |
| `deleted` | removed from the queue by **this client's** `qdel` |
| `missing` | the queue host has no record of it |

`missing` is the ambiguous one. It means someone else deleted the job, or the
failure aged out of the host's 1000-record history, or the job was submitted to
a different queue host than the one being queried. It is *not* treated as
final — the job keeps getting re-checked, and a whole host's worth of jobs
never flips to `missing` at once (that pattern is read as a bad read of the
host's state files rather than as mass deletion). That last rule is now a
backstop rather than the primary defense: the queue host writes its state files
atomically, so a reader can no longer catch one mid-write. It still covers a
queue host running an older version.

Filters:

- `--status` takes any of the names above, repeated or comma-separated, plus
  the aliases `active` (submitted/queued/running), `done` and `all`.
- `--queue` filters by queue name, also repeated or comma-separated. Names are
  normalized the same way the queue host normalizes them at submit, and matched
  case-insensitively, so what you can submit to is what you can filter on.
- `--since` / `--until` accept `YYYY-MM-DD`, `'YYYY-MM-DD HH:MM[:SS]'`, or a
  relative span (`30m`, `24h`, `7d`, `2w`). They filter on submission time, in
  **your** local timezone. As an upper bound a bare date means the *end* of
  that day, so `--until 2026-07-30` includes the 30th.
- `--limit` / `-n` caps the rows (default 50; `0` for all). A collapsed batch is
  **one row** however many jobs it holds — see below.

#### Batches

Submitting a folder of work means a shell loop, one `submit` per subdirectory.
At a hundred-odd jobs those drown out everything else in `jobs`. Tag them at
submit and they collapse to one row:

```bash
cd ffpopt
awsqe-client submit --payload-glob 'IDC*' --queue production --priority -100 --mps \
    "source ~/flowrc && cd \$PAYLOAD_DIR && python run_fe.py"
```

One invocation is one batch, so the tag is derived for you
(`IDC-20260802-091402`); `--array NAME` overrides it. The equivalent shell loop
still works and takes `--array` per job:

```bash
for IDC in IDC*; do
    awsqe-client submit --array ffpopt-IDC --payload "${PWD}/$IDC/" ... ;
done
```

```
$ awsqe-client jobs
SUBMITTED             ARRAY                       JOBS  QUEUE         STATUS
2026-08-02 09:14:02   ffpopt-IDC                   142  production    130 completed · 9 failed · 3 running
2026-08-01 16:40:11   sweep-b                       20  fast-gpus     20 completed

SUBMITTED             JOB                     STATUS      HOST      QUEUE         DUR       CMD
2026-08-02 11:02:55   20260802-110255-7ac1e0  running     eci7      default       -         python train.py
163 of 163 tracked job(s); 162 in 2 batch(es).
```

- Grouping is **on by default**. Untagged jobs list exactly as they always have,
  below the batch rows.
- `SUBMITTED` on a batch row is when you fired it off — the *earliest* of its
  jobs — but batches sort by their most recent, so one still being submitted
  stays at the top.
- `--array NAME` drills into one batch and lists its jobs individually;
  `--expand` (or `--no-group`) turns grouping off entirely. `--fetch-logs`
  implies expansion, since a log path has nowhere to go on a batch row.
- The other filters compose, and the batch row reflects what survived them:
  `jobs --array ffpopt-IDC --status failed --fetch-logs` is the practical way to
  read the tracebacks out of a batch.
- A batch that went to more than one queue shows `*` in the `QUEUE` column
  rather than picking one of them.

`--payload-glob` does the loop inside one process, which is where the real cost
was: 105 payloads submitted the old way meant 105 interpreter startups, 105
serial tar-and-upload round trips to S3, and 105 separate SSH connections. Here
the uploads run in a pool (`-j`, default 4), the enqueues share one connection,
and the ledger is written once.

- `--dry-run` lists what would be submitted and stops — cheap insurance before
  firing 105 jobs at the queue.
- Job ids are minted up front, so they follow directory order even though the
  uploads finish out of order.
- **Partial failure is reported, never rolled back.** If payload 60 of 105 fails
  to upload, the other 104 are already real jobs on the queue; both sets are
  printed by name and the exit status is non-zero so a driving script notices.
- Only directories match; a stray file caught by `IDC*` is skipped.

Names are letters, digits, `.`, `_` and `-`, up to 64 characters. An unusable
name is **rejected** rather than quietly rewritten — unlike a queue name, an
array name is something you type back in at `jobs --array` and (once the queue
host carries the tag) `qdel --array`, and a silent rewrite at submit would leave
those matching nothing with nothing on screen to explain why.

Reusing a name is legal and appends to that batch; `--since` separates the runs.

#### Job logs

`--fetch-logs` copies each displayed job's log off the worker that ran it into
`~/.awsqe/client/logs/<job_id>.log`, and prints the local path under the row:

```
2026-08-01 10:51:39   20260731-181013-cfd2e8  completed   eci3      production    00:00:06  python run_fe.py
                      log: /home/you/.awsqe/client/logs/20260731-181013-cfd2e8.log
```

To read one job's log, `--cat` prints it straight to the screen and `--log`
prints just its path — both fetch first if the log isn't cached yet:

```bash
awsqe-client jobs --cat 20260731-1810              # the log itself
awsqe-client jobs --cat 20260731-1810 | grep Error # nothing else goes to stdout
less "$(awsqe-client jobs --log 20260731-1810)"    # the path, for other tools
```

Fetching goes **client → worker** over `scp`, not through the queue host, so it
needs SSH access to the ecis (the same access `awsqe-client tail`/`stop`/`status`
already assume). It's opt-in because it costs one connection per job, and it's
scoped to the rows actually displayed — `-n 5 --fetch-logs` fetches five logs,
not your whole history.

Already-fetched logs are skipped, but the cache is keyed on **which worker and
which finish time**, not just the job id. A requeued job truncates its log and
may land on a different worker, so a rerun re-fetches rather than serving the
previous attempt. Running jobs are always re-fetched, since their log is still
being written. When a worker no longer has the log — recycled, or
`manager_jobs` cleaned — that's recorded so it isn't retried every run:

```
[WARN] 19990101-000000-deadbe: no log left on eci3 (worker recycled or cleaned)
```

The cache is capped at 512 MB, dropping the oldest first, and `--forget` deletes
a job's cached log along with its ledger entry.

The ledger holds 2000 jobs, dropping the oldest *finished* ones past that —
jobs still in flight are never evicted. `--forget <job-id-or-prefix>` and
`--forget-before <when>` remove entries by hand; both only stop tracking, they
never cancel anything (use `qdel` for that).

`awsqe-client info --job-id <id-or-prefix>` refreshes a single tracked job
without needing to `cd` to its payload directory, and rewrites that payload's
`run.info` if it still exists. It's also how to re-check a job the list already
considers finished, since `jobs` doesn't re-query those.

Two caveats worth knowing: the ledger is **per machine**, so submitting from a
laptop and a workstation gives each its own half of the picture; and a queue
host running an older `awsqe-host` has no batched lookup, so the refresh falls
back to one round trip per job (it says so, once, on stderr).

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

### How concurrent state access works

Every RPC runs in its own process — `awsqe-client` invokes
`ssh <queue-host> awsqe-host rpc` per call — so `submit`, `qdel` and
`requeue-deferred` are genuinely concurrent with the monitor daemon. Two rules
keep that safe:

- **Writes are atomic.** Each state file is written to `*.tmp.<pid>` and then
  `os.replace`d over the target, so a reader sees either the whole old file or
  the whole new one. Readers never block and never need the lock.
- **Mutations are serialized** by `~/.awsqe/host/state.lock`, taken by the
  monitor and by every RPC handler around each read-modify-write, with the read
  happening *inside* the lock. Without that, a burst of submits landing while
  the monitor dispatched could be acknowledged to the submitter and then erased
  by the monitor writing back its older copy.

Critical sections are local file I/O only — no SSH, no email — so contention is
microseconds even during a 200-job burst. The lock is a `flock`, which the
kernel releases if the holder dies, so a crashed process cannot wedge the host.

`state.lock` is *not* `lock`: that one is the daemon singleton ("only one
monitor may run"), held for the monitor's entire lifetime.

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
~/.awsqe/host/lock             # daemon singleton: "only one monitor may run"
~/.awsqe/host/state.lock       # serializes state mutation; see below
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
- `src/awsqueueengine/client/` — `awsqe-client` CLI, submit, run.info, tracked-job ledger, RPC transport
- `src/awsqueueengine/host/` — `awsqe-host` CLI, monitor, job control, migration, daemon
- `src/awsqueueengine/cli.py` — legacy `awsqueueengine` shim that dispatches to one of the two
- `scripts/` — local-only utilities (smoke tests for remote queue host validation)
- `tests/` — unit + subprocess tests
- `setup.py` — packaging

## Tests

```bash
pytest              # the default suite
pytest -m slow      # plus the cross-process concurrency test
```

`-m slow` covers `tests/test_host_state_concurrency.py`, which spawns real
submitter and dispatcher processes to prove the state lock actually excludes
across processes. It takes a second or two and is excluded from the default run
by `pytest.ini`.
