Quickstart
==========

Assumes you have :doc:`installed <installation>` the client and set
``queue-host`` in :doc:`configuration`.

Most days you will only need these.

Submit
------

.. code-block:: bash

   awsqe-client submit --payload ./my_payload "cd $PAYLOAD_DIR && bash run.sh"
   awsqe-client submit --queue fast-gpus "python train.py"
   awsqe-client submit --priority 25 "python train.py --epochs 10"
   awsqe-client submit --preempt --priority 999 "bash urgent-job.sh"

   # Override the configured queue host for one command:
   awsqe-client submit --queue-host other-queue "python sweep.py"

See :doc:`submitting`.

Inspect the queue host
----------------------

These are **global** views — everyone's jobs, not just yours.

.. code-block:: bash

   awsqe-client list                  # queued jobs
   awsqe-client qstat                 # running jobs, with elapsed HH:MM:SS
   awsqe-client failed                # failures, newest first, with a reason
   awsqe-client failed --job-id JOB --log   # plus that job's captured log tail
   awsqe-client deferred              # jobs past the submit-failure limit
   awsqe-client requeue-deferred --all
   awsqe-client enable-host           # show active host cooldowns (no args)
   awsqe-client enable-host eci17     # release a host from cooldown early

Track your own jobs
-------------------

A local list, so it works without a payload directory and stays useful offline.

.. code-block:: bash

   awsqe-client jobs                        # everything this machine submitted
   awsqe-client jobs --status active        # submitted, queued or running
   awsqe-client jobs --status failed --since 7d
   awsqe-client jobs --queue zeke-queue     # one queue (repeatable, comma-separated)
   awsqe-client jobs --array ffpopt-IDC     # one batch's jobs, individually
   awsqe-client jobs --expand               # never collapse batches into one row
   awsqe-client jobs --no-refresh           # skip the queue-host round trip
   awsqe-client jobs --fetch-logs           # pull each shown job's log off its worker
   awsqe-client jobs --cat 20260730-1415    # print one job's log to the screen
   awsqe-client jobs --log 20260730-1415    # print the local path to one job's log
   awsqe-client jobs --forget 20260730-1415 # stop tracking (does NOT cancel)
   awsqe-client info --job-id 20260730-1415 # refresh one job without a payload dir

See :doc:`tracking-jobs`.

Delete queued jobs
------------------

Addressed by the job id ``list`` prints as ``[job=...]``:

.. code-block:: bash

   awsqe-client qdel 20260730-141530-a1b2c3
   awsqe-client qdel 20260730-141530-a1b2c3 20260730-141602-9f0e11
   awsqe-client qdel 20260730-1415       # any unique prefix of a job id
   awsqe-client qdel --queue fast-gpus   # every queued job in one queue

Worker-host operations
----------------------

These SSH to the worker directly, not through the queue host.

.. code-block:: bash

   awsqe-client status                # ps probe of every host's MANAGER_TAG state
   awsqe-client tail eci17            # tail the most recent job log on a worker
   awsqe-client stop eci17            # kill managed job(s) on a worker
   awsqe-client where                 # probe scratch space on every worker
   awsqe-client info -p ./my_payload  # refresh run.info from queue-host state
