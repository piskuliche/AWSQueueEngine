# AWSQueueEngine

A Slurm-like SSH job manager for AWS GPU hosts, with payload staging and monitoring.

## Installation

From the project root:

```bash
pip install .
```

## Usage

After installation, use the CLI:

```bash
awsqueueengine status
awsqueueengine submit --payload ./my_payload "cd $PAYLOAD_DIR && bash run.sh"
awsqueueengine submit --priority 25 "python train.py --epochs 10"
awsqueueengine submit --hosts eci17 --priority 100 "bash pinned-job.sh"
awsqueueengine submit --hosts eci16 --hosts eci18 "bash host-allowlist-job.sh"
awsqueueengine submit --preempt --priority 999 "bash urgent-job.sh"
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
Use `--hosts` multiple times (or comma-separated values) to target multiple hosts.
Use `--preempt` to allow a queued job to interrupt a currently running managed job
when no free eligible host is available. The interrupted job is requeued and restarted.
`qstat` lists monitor-tracked running jobs and elapsed runtime (`HH:MM:SS`).


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
- `setup.py` - Packaging script
- `README.md` - This file
# AWSQueueEngine
A simple queue engine for AWS Resources.
