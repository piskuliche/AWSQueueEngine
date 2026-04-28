# AWSQueueEngine

A Slurm-like SSH job manager for AWS GPU hosts, with payload staging and monitoring.

## Installation

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
export AWSQUEUEENGINE_HOSTS_FILE="/home/ubuntu/queue_hosts.txt"
nohup awsqueueengine start-monitor >> ~/aws_queue_manager.log 2>&1 &
```

The hosts file should list every worker the monitor can use. You can also define
named host sets on the queue host:

```bash
export AWSQUEUEENGINE_HOST_SET_FAST_GPUS="eci1 eci2 eci3"
export AWSQUEUEENGINE_HOSTS_FILE_LARGE_MEM="/home/ubuntu/large_mem_hosts.txt"
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
awsqueueengine status --hosts-file ~/queue_hosts.txt
awsqueueengine submit --payload ./my_payload "cd $PAYLOAD_DIR && bash run.sh"
awsqueueengine submit --queue-host queue-manager --payload ./my_payload "cd $PAYLOAD_DIR && bash run.sh"
awsqueueengine submit --queue-host queue-manager --host-set fast-gpus "python train.py"
awsqueueengine submit --hosts-file ~/queue_hosts.txt --hosts eci17 "bash pinned-job.sh"
awsqueueengine submit --priority 25 "python train.py --epochs 10"
awsqueueengine submit --hosts eci17 --priority 100 "bash pinned-job.sh"
awsqueueengine submit --hosts eci16 --hosts eci18 "bash host-allowlist-job.sh"
awsqueueengine submit --preempt --priority 999 "bash urgent-job.sh"
awsqueueengine requeue-running --hosts eci17
awsqueueengine requeue-running --all
awsqueueengine list
awsqueueengine qstat
awsqueueengine qdel 2
awsqueueengine qdel 1 3
awsqueueengine start-monitor
awsqueueengine start-monitor --hosts-file ~/queue_hosts.txt
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
Use `--hosts` multiple times (or comma-separated values) to target multiple hosts.
Use `start-monitor --hosts-file <path>` to source monitor hosts from a file and reload
changes automatically while the monitor is running.
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
host should run the monitor and own the queue files. If you use host restrictions
with remote submit, validation happens on the queue host; set
`AWSQUEUEENGINE_HOSTS_FILE=/path/to/queue_hosts.txt` there if you do not want to
use the built-in host list. Configure an S3 lifecycle rule for the payload prefix
to clean up uploaded archives.

### Named host sets

Named host sets let users submit to predetermined host pools while still using the
same queue and the same monitor. Define them on the queue host, then submit with
`--host-set <name>`:

```bash
export AWSQUEUEENGINE_HOST_SET_FAST_GPUS="eci1 eci2 eci3"
export AWSQUEUEENGINE_HOSTS_FILE_LARGE_MEM="/home/ubuntu/large_mem_hosts.txt"

awsqueueengine submit --queue-host queue-manager --host-set fast-gpus "python train.py"
```

Host set names are normalized for environment variables: `fast-gpus` maps to
`AWSQUEUEENGINE_HOST_SET_FAST_GPUS` or `AWSQUEUEENGINE_HOSTS_FILE_FAST_GPUS`.
The monitor should still run once over the full host pool; a named host set is
stored as that job's host allowlist.


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
