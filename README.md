# AWSQueueEngine

A Slurm-like SSH job manager for AWS GPU hosts, with payload staging and monitoring.

## Installation

Requires **Python 3.10+** on both the local submitter and the queue host
(the codebase uses PEP 604 union annotations).

Install the package anywhere you will run the CLI:

```bash
pip install .
```

### Local submitter setup

Install AWSQueueEngine locally and make sure you can SSH to the queue host:

```bash
pip install .
ssh queue-manager 'awsqueueengine status-monitor'
```

For S3-backed payload submit, configure AWS credentials locally with write access
to the payload bucket, then set:

```bash
export AWSQUEUEENGINE_S3_BUCKET="my-queue-payload-bucket"
export AWSQUEUEENGINE_S3_PREFIX="awsqueueengine/payloads"  # optional
```

Submit through the remote queue host:

```bash
awsqueueengine submit --queue-host queue-manager --payload ./my_payload "cd $PAYLOAD_DIR && bash run.sh"
```

### Remote queue host setup

Install AWSQueueEngine on the queue host. This machine owns the queue files and
should be the only place running the monitor:

```bash
pip install .
cat > /home/ubuntu/awsqueueengine_queues.json <<'JSON'
{
  "default": ["eci1", "eci2", "eci3"],
  "fast-gpus": ["eci1", "eci2"],
  "large-mem": ["eci3"]
}
JSON
export AWSQUEUEENGINE_QUEUES_FILE="/home/ubuntu/awsqueueengine_queues.json"
nohup awsqueueengine start-monitor >> ~/aws_queue_manager.log 2>&1 &
```

The queue config is the single source of truth for worker assignment. Edit this
file to move hosts between user queues; the monitor reloads it while running.
For simple static setups, use one environment variable instead of a file:

```bash
export AWSQUEUEENGINE_QUEUES="default=eci1,eci2,eci3;fast-gpus=eci1,eci2"
```

### Worker host setup

Each worker host must be reachable by SSH from the queue host and have scratch
space under the configured scratch roots. For S3-backed payloads, workers also
need the AWS CLI and IAM permissions to read the payload bucket/prefix:

```bash
aws s3 ls s3://my-queue-payload-bucket/awsqueueengine/payloads/
```

## Usage

After installation, use the CLI:

```bash
awsqueueengine status
awsqueueengine submit --payload ./my_payload "cd $PAYLOAD_DIR && bash run.sh"
awsqueueengine submit --queue-host queue-manager --payload ./my_payload "cd $PAYLOAD_DIR && bash run.sh"
awsqueueengine submit --queue-host queue-manager --queue fast-gpus "python train.py"
awsqueueengine submit --queue large-mem "python analyze.py"
awsqueueengine submit --priority 25 "python train.py --epochs 10"
awsqueueengine submit --preempt --priority 999 "bash urgent-job.sh"
awsqueueengine requeue-running --hosts eci17
awsqueueengine requeue-running --all
awsqueueengine list
awsqueueengine qstat
awsqueueengine qdel 2
awsqueueengine qdel 1 3
awsqueueengine start-monitor
awsqueueengine status-monitor
awsqueueengine stop-monitor
awsqueueengine tail eci3
awsqueueengine stop eci3
awsqueueengine clear
awsqueueengine --test-email-connection
```

`submit --high-priority` is still supported for backward compatibility and maps to
priority `100`. If both `--priority` and `--high-priority` are supplied, `--priority`
takes precedence.
Use `--queue <name>` to submit to a user queue. Jobs store the queue name, and
the monitor assigns the concrete worker host at dispatch time from the current
queue config. `--hosts` and `--host-set` remain for legacy scripts, but new
remote behavior should prefer queues.
Use `--preempt` to allow a queued job to interrupt a currently running managed job
when no free eligible host is available. The interrupted job is requeued and restarted.
Use `requeue-running` to kill monitor-tracked running job(s) and requeue them back to
their original host with priority `100`, preserving the remote payload path.
`qstat` lists monitor-tracked running jobs and elapsed runtime (`HH:MM:SS`).
When jobs finish, the monitor appends completion records to
`~/.aws_slurm_like_completed.json` with the `qstat` fields plus final duration
and timestamps (`started_at`, `finished_at`).

## Remote submit with S3 payloads

`submit --queue-host <host>` lets you run the CLI locally while enqueueing on a
remote queue-manager machine. With `--payload`, the local CLI archives the payload
directory, uploads it to S3, then SSHes to the queue host to enqueue a job that
contains the S3 payload URI. The monitor on the queue host later asks the selected
worker to download and extract that archive before running the job.

Local submitter requirements:

```bash
export AWSQUEUEENGINE_S3_BUCKET="my-queue-payload-bucket"
export AWSQUEUEENGINE_S3_PREFIX="awsqueueengine/payloads"  # optional
awsqueueengine submit --queue-host queue-manager --payload ./my_payload "cd $PAYLOAD_DIR && bash run.sh"
```

The local submitter needs AWS credentials with write access to the bucket/prefix.
Worker hosts need the AWS CLI and read access to the same bucket/prefix. The queue
host should run the monitor and own the queue files. Queue validation happens on
the queue host against `AWSQUEUEENGINE_QUEUES_FILE` or `AWSQUEUEENGINE_QUEUES`.
Configure an S3 lifecycle rule for the payload prefix to clean up uploaded archives.

### User queues

User queues let users submit to predetermined host pools while still using the
same job queue and monitor. Define them on the queue host, then submit with
`--queue <name>`:

```bash
export AWSQUEUEENGINE_QUEUES_FILE="/home/ubuntu/awsqueueengine_queues.json"

awsqueueengine submit --queue-host queue-manager --queue fast-gpus "python train.py"
```

`AWSQUEUEENGINE_QUEUES_FILE` and `AWSQUEUEENGINE_QUEUES` are mutually exclusive.
The file form is preferred for live changes because the monitor reloads it each
poll; environment variables are read from the running process environment.


Suggested to run with:

```bash
 nohup awsqueueengine start-monitor >> ~/aws_queue_manager.log 2>&1 &
```

Or, for development, run directly:

```bash
python -m cli list
```

## Email Alerts (Mailtrap API)

Set these environment variables before starting the monitor:

```bash
export AWSQUEUEENGINE_MAILTRAP_TOKEN="<your-mailtrap-token>"
export AWSQUEUEENGINE_MAILTRAP_SENDER_EMAIL="hello@piskulich.com"
export AWSQUEUEENGINE_MAILTRAP_SENDER_NAME="Queue Monitor"
export AWSQUEUEENGINE_MAILTRAP_CATEGORY="Integration Test"
export AWSQUEUEENGINE_ALERT_TO="you@example.com,team@example.com"
export AWSQUEUEENGINE_ALERT_DAILY_EMAIL_LIMIT="150"
export AWSQUEUEENGINE_JOB_FAIL_ALERT_COOLDOWN_SECONDS="900"
```

When configured, the monitor sends email:

1. When a queued job fails to start on a host.
2. Once when queue depth drops below 10 (fires on transition into low-queue state).
3. Once when queue depth reaches 0 (fires on transition into empty state).
4. Once per calendar day when the monitor detects a new date, with a status summary.

Email protection:
- Total outgoing emails are capped per day (`AWSQUEUEENGINE_ALERT_DAILY_EMAIL_LIMIT`, default `150`).
- Job-failure emails are rate-limited with a cooldown (`AWSQUEUEENGINE_JOB_FAIL_ALERT_COOLDOWN_SECONDS`, default `900` seconds).



## Project Structure

- `src/` - Main package modules
- `docs/` - Sphinx documentation source and build helpers
- `setup.py` - Packaging script
- `README.md` - This file

## Documentation

Build the Sphinx HTML documentation from the project root with:

```bash
pip install -r docs/requirements.txt
sphinx-build -b html docs docs/_build/html
```

The generated site will be written to `docs/_build/html/index.html`.
# AWSQueueEngine
A simple queue engine for AWS Resources.
