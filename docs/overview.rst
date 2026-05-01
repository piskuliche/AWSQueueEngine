Overview
========

What It Does
------------

AWSQueueEngine acts like a small, Slurm-inspired job manager for a fixed set of SSH-accessible hosts.
Jobs are queued locally, dispatched remotely, and tracked through a monitor process.

Core capabilities include:

* Queueing jobs with integer priorities.
* Submitting jobs to user queues whose hosts are resolved at dispatch time.
* Uploading payload directories to remote scratch space.
* Monitoring running jobs and recording completion metadata.
* Preempting lower-priority jobs and requeueing them to their original hosts.
* Requeueing running jobs back to their original hosts with priority ``100``.

Main Components
---------------

``awsqueueengine.cli``
  Command-line entry point for queue operations, monitoring, status inspection, and job control.

``awsqueueengine.monitor``
  Monitor loop that polls host state, launches queued jobs, handles alerts, and implements preemption.

``awsqueueengine.queue``
  Persistent queue storage and priority-aware dequeue logic.

``awsqueueengine.queue_config``
  User queue to worker-host configuration loaded from one file or one environment variable.

``awsqueueengine.job_control``
  Remote submission, job termination, log tailing, and payload environment setup.

``awsqueueengine.host_status``
  Remote process inspection across all configured hosts.

``awsqueueengine.running_state`` and ``awsqueueengine.completion_state``
  Persistent tracking for active and completed jobs.

Data Files
----------

The queue engine stores its local state in the current user's home directory:

* ``~/.aws_slurm_like_queue.json``
* ``~/.aws_slurm_like_running.json``
* ``~/.aws_slurm_like_completed.json``
* ``~/.aws_slurm_like_monitor_state.json``
* ``~/awsqueueengine.pid``
