Usage Guide
===========

Installation
------------

Install the package anywhere you will run the CLI:

.. code-block:: bash

   pip install .

There are three roles in the shared remote-submit setup:

* Local submitter: the user's laptop or workstation.
* Queue host: the single remote machine that owns the queue files and runs the monitor.
* Worker hosts: the AWS machines where jobs actually execute.

Local Submitter Setup
~~~~~~~~~~~~~~~~~~~~~

Install AWSQueueEngine locally and make sure you can SSH to the queue host:

.. code-block:: bash

   pip install .
   ssh queue-manager 'awsqueueengine status-monitor'

For S3-backed payload submit, configure AWS credentials locally with write access
to the payload bucket, then set:

.. code-block:: bash

   export AWSQUEUEENGINE_S3_BUCKET="my-queue-payload-bucket"
   export AWSQUEUEENGINE_S3_PREFIX="awsqueueengine/payloads"  # optional

Then submit through the remote queue host:

.. code-block:: bash

   awsqueueengine submit --queue-host queue-manager --payload ./my_payload "cd $PAYLOAD_DIR && bash run.sh"

Queue Host Setup
~~~~~~~~~~~~~~~~

Install AWSQueueEngine on the queue host. This machine owns the queue state in
the queue user's home directory and should be the only place running the monitor:

.. code-block:: bash

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

The queue config is the single source of truth for worker assignment. Edit this
file to move hosts between user queues; the monitor reloads it while running.
For simple static setups, use one environment variable instead of a file:

.. code-block:: bash

   export AWSQUEUEENGINE_QUEUES="default=eci1,eci2,eci3;fast-gpus=eci1,eci2"

Users can then submit to those pools without knowing the individual host names:

.. code-block:: bash

   awsqueueengine submit --queue-host queue-manager --queue fast-gpus "python train.py"

Worker Host Setup
~~~~~~~~~~~~~~~~~

Each worker host must be reachable by SSH from the queue host and have enough
scratch space under the configured scratch roots. For S3-backed payloads, workers
also need the AWS CLI and IAM permissions to read the payload bucket/prefix:

.. code-block:: bash

   aws s3 ls s3://my-queue-payload-bucket/awsqueueengine/payloads/

The queue host launches jobs over SSH and workers download payload archives
directly from S3 before running the command with ``PAYLOAD_DIR`` set.

Basic Commands
--------------

.. code-block:: bash

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
   awsqueueengine qdel 20260730-141530-a1b2c3
   awsqueueengine qdel --queue fast-gpus
   awsqueueengine qdel --index 2
   awsqueueengine start-monitor
   awsqueueengine stop-monitor

Deleting Queued Jobs
--------------------

``qdel`` selects jobs by the job id ``list`` prints as ``[job=...]``, not by the
number in its left column. That number is a render-time position and it moves
constantly — every deletion renumbers the entries after it, the monitor dequeues
the highest-priority job from anywhere in the list, requeues insert at the front,
and other users' submits append. Deleting several jobs from one listing by
position therefore removes the wrong ones.

.. code-block:: bash

   awsqueueengine qdel 20260730-141530-a1b2c3                      # one job
   awsqueueengine qdel 20260730-141530-a1b2c3 20260730-141602-9f0e11
   awsqueueengine qdel 20260730-1415                               # unique prefix
   awsqueueengine qdel --queue fast-gpus                           # a whole queue
   awsqueueengine qdel --index 2                                   # by position

The three selectors cannot be combined in a single command. Nothing is removed
unless every selector resolves, so an unknown or ambiguous job id leaves the
queue untouched. ``qdel`` only removes *queued* jobs; use ``stop`` for one that
has already started.

Tracking Your Own Jobs
----------------------

``list``, ``qstat`` and ``failed`` are global views — the queue host records no
notion of who submitted what. ``awsqe-client jobs`` answers "how are *my* jobs
doing?" from a local ledger at ``~/.awsqe/client/jobs.json``, appended on every
``awsqe-client submit`` (including submits with no payload directory, which
write no ``run.info`` anywhere).

.. note::

   This subcommand is ``awsqe-client``-only. There is no ``awsqueueengine jobs``
   equivalent: the ledger is a purely client-side file, so there is nothing for
   the legacy shim to route to a host.

.. code-block:: bash

   awsqe-client jobs                          # newest first, refreshed from the queue host
   awsqe-client jobs --status active          # submitted, queued or running
   awsqe-client jobs --status failed --since 7d
   awsqe-client jobs --status queued,running --until 2026-07-30
   awsqe-client jobs --queue zeke-queue        # one queue
   awsqe-client jobs --queue gpu,bigmem        # several
   awsqe-client jobs --no-refresh             # local state only, no SSH
   awsqe-client jobs --fetch-logs             # pull shown jobs' logs off their workers
   awsqe-client jobs --log 20260730-1415      # print the local path to one log
   awsqe-client jobs -n 10                    # ten most recent (0 for all)
   awsqe-client jobs --forget 20260730-1415   # stop tracking; does NOT cancel
   awsqe-client jobs --forget-before 2026-01-01
   awsqe-client info --job-id 20260730-1415   # refresh one job, no payload dir needed

Statuses are ``submitted`` (not yet refreshed), ``queued``, ``running``,
``completed``, ``failed``, ``unknown`` (finished without a provable exit
status), ``deleted`` (removed by this client's ``qdel``) and ``missing`` (the
queue host has no record — deleted by someone else, aged out of the failure
history, or submitted to a different queue host). ``--status`` accepts these
repeated or comma-separated, plus the aliases ``active``, ``done`` and ``all``.

``--queue`` filters by queue name, also repeated or comma-separated. Names go
through the same normalization the queue host applies at submit and are matched
case-insensitively, so what you can submit to is what you can filter on.

``--since`` and ``--until`` accept ``YYYY-MM-DD``, ``'YYYY-MM-DD HH:MM[:SS]'``,
or a relative span (``30m``, ``24h``, ``7d``, ``2w``), and filter on submission
time in the submitter's local timezone. As an upper bound a bare date means the
*end* of that day, so ``--until 2026-07-30`` includes the 30th.

The ledger is per machine and holds 2000 jobs, evicting only the oldest
*finished* ones. An unreachable queue host degrades to last-known statuses with
a warning rather than an error, and a queue host predating the batched lookup
falls back to one round trip per job.

``--fetch-logs`` copies each displayed job's log from the worker that ran it
into ``~/.awsqe/client/logs/``, going client-to-worker over ``scp`` rather than
through the queue host. It is opt-in (one connection per job) and scoped to the
displayed rows. The cache is keyed on the worker and finish time, so a requeued
job re-fetches instead of serving the previous attempt, and running jobs are
always re-fetched. A log the worker no longer has is recorded as such so it is
not retried on every run. The cache is capped at 512 MB.

.. warning::

   The exit status recorded for a job is the **shell's** exit status, which for
   a ``;``-chained command is the status of the *last* one. ``python run.py;
   rm -rf $PAYLOAD_DIR`` reports success whenever the ``rm`` succeeds, even if
   the job crashed. Use ``&&`` between the steps, or capture the status
   (``rc=$?``) before the cleanup and ``exit $rc``.

Queue Targeting
---------------

Use ``--queue`` to submit to a user queue. Jobs store the queue name, and the
monitor assigns the concrete worker host at dispatch time from the current queue
config.

.. code-block:: bash

   awsqueueengine submit --queue fast-gpus "python train.py"
   awsqueueengine submit --queue large-mem "python analyze.py"

Use one queue source: either ``AWSQUEUEENGINE_QUEUES_FILE`` or
``AWSQUEUEENGINE_QUEUES``. The file form is preferred when you want to add,
remove, or move hosts without restarting the monitor.

.. code-block:: bash

   # Preferred: live-reloaded file.
   export AWSQUEUEENGINE_QUEUES_FILE="/home/ubuntu/awsqueueengine_queues.json"

   # Or, for static process environment config:
   export AWSQUEUEENGINE_QUEUES="default=eci1,eci2;fast-gpus=eci1"

``--hosts`` and ``--host-set`` remain for legacy scripts, but new remote
behavior should prefer queues.

Payloads
--------

When a payload directory is supplied, the engine stages it to remote scratch space and runs the
job with ``PAYLOAD_DIR`` set to the remote payload path.

.. code-block:: bash

   awsqueueengine submit --payload ./my_payload "cd $PAYLOAD_DIR && bash run.sh"

If a running job is requeued or preempted, the queue engine preserves the remote payload path so
the restarted job still receives the correct ``PAYLOAD_DIR``.

Remote Submit with S3 Payloads
------------------------------

``submit --queue-host <host>`` lets you run the CLI locally while enqueueing on a
remote queue-manager host. If ``--payload`` is provided, the local CLI creates a
``.tar.gz`` archive, uploads it to S3, and forwards the submit over SSH with the
S3 URI. The monitor on the queue host then has the selected worker download and
extract the archive before launching the job.

Local submitters need write access to ``AWSQUEUEENGINE_S3_BUCKET``:

.. code-block:: bash

   export AWSQUEUEENGINE_S3_BUCKET="my-queue-payload-bucket"
   export AWSQUEUEENGINE_S3_PREFIX="awsqueueengine/payloads"  # optional
   awsqueueengine submit --queue-host queue-manager --payload ./my_payload "cd $PAYLOAD_DIR && bash run.sh"

Worker hosts need the AWS CLI and read access to the same bucket/prefix. Queue
validation for remote submit happens on the queue host against
``AWSQUEUEENGINE_QUEUES_FILE`` or ``AWSQUEUEENGINE_QUEUES``. Configure an S3
lifecycle rule for uploaded payload archives.

User queues can be configured on the queue host and selected during submit:

.. code-block:: bash

   export AWSQUEUEENGINE_QUEUES_FILE="/home/ubuntu/awsqueueengine_queues.json"
   awsqueueengine submit --queue-host queue-manager --queue fast-gpus "python train.py"

``AWSQUEUEENGINE_QUEUES_FILE`` and ``AWSQUEUEENGINE_QUEUES`` are mutually
exclusive. The monitor reloads the file source each poll.

Preemption and Requeue
----------------------

``--preempt`` allows a queued job to interrupt a lower-priority running managed job when there is
no free eligible host. The interrupted job is requeued and pinned back to its original host.

``requeue-running`` kills the current managed job on one or more hosts and immediately requeues the
tracked job metadata back to the same host at priority ``100``. The requeue happens before the kill
result is evaluated, so the item is preserved even if the kill command fails.

Building The Docs
-----------------

From the project root:

.. code-block:: bash

   sphinx-build -b html docs docs/_build/html
