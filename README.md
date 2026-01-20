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
awsqueueengine list
awsqueueengine start-monitor
awsqueeuengine status-monitor
awsqueueengine stop-monitor
awsqueueengine tail eci3
awsqueueengine stop eci3
awsqueueengine clear
```


Suggested to run with:

```bash
 nohup awsqueueengine start-monitor >> ~/aws_queue_manager.log 2>&1 &
```

Or, for development, run directly:

```bash
python -m cli list
```



## Project Structure

- `src/` - Main package modules
- `setup.py` - Packaging script
- `README.md` - This file
# AWSQueueEngine
A simple queue engine for AWS Resources.
