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
awsqueueengine start
awsqueueengine tail eci3
awsqueueengine stop eci3
awsqueueengine clear
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
