Submitting jobs
===============

Priorities
----------

Jobs carry an integer priority; the monitor dequeues the highest-priority
eligible job from anywhere in the queue.

.. code-block:: bash

   awsqe-client submit --priority 25 "python train.py --epochs 10"

``--high-priority`` is still supported for backward compatibility and maps to
priority ``100``. If both are supplied, ``--priority`` wins.

Queue targeting
---------------

``--queue <name>`` submits to a user queue. Jobs store the *queue name*, and
the monitor assigns the concrete worker host at dispatch time from the current
queue config — so hosts can be moved between queues without disturbing work
that is already queued.

.. code-block:: bash

   awsqe-client submit --queue fast-gpus "python train.py"
   awsqe-client submit --queue large-mem "python analyze.py"

``--hosts`` and ``--host-set`` remain for legacy scripts, but new submissions
should prefer queues. Queue definitions live on the queue host — see
:doc:`queue-host`.

Payloads
--------

With ``--payload <dir>``, the client archives the directory, uploads it to S3,
and asks the queue host to enqueue a job referencing the S3 URI. The monitor
later has the selected worker download and extract the archive before running
the command, with ``PAYLOAD_DIR`` set to the remote payload path.

.. code-block:: bash

   awsqe-client submit --payload ./my_payload "cd $PAYLOAD_DIR && bash run.sh"

One-time setup on the submitter:

.. code-block:: bash

   awsqe-client config set s3.bucket  my-queue-payload-bucket
   awsqe-client config set s3.prefix  awsqueueengine/payloads   # optional

The submitter needs AWS credentials with **write** access to the bucket and
prefix; worker hosts need **read** access to the same prefix. The queue host
validates the queue name against its own configuration, not yours. Configure an
S3 lifecycle rule on the payload prefix to clean up old archives.

If a running job is requeued or preempted, the remote payload path is
preserved, so the restarted job still receives the correct ``PAYLOAD_DIR``.

Preemption and requeue
----------------------

``--preempt`` lets a queued job interrupt a lower-priority running managed job
when no free eligible host is available. The interrupted job is requeued and
pinned back to its original host.

.. code-block:: bash

   awsqe-client submit --preempt --priority 999 "bash urgent-job.sh"

``requeue-running`` kills the managed job on one or more hosts and requeues the
tracked metadata back to the same host at priority ``100``. The requeue happens
*before* the kill result is evaluated, so the item survives even if the kill
command fails.

.. code-block:: bash

   awsqe-host requeue-running --hosts eci17
   awsqe-host requeue-running --all

Deleting queued jobs
--------------------

``qdel`` selects jobs by the job id ``list`` prints as ``[job=...]``, not by the
number in its left column. That number is a render-time position and it moves
constantly — every deletion renumbers the entries after it, the monitor dequeues
the highest-priority job from anywhere in the list, requeues insert at the front,
and other users' submits append. Deleting several jobs from one listing by
position therefore removes the wrong ones.

.. code-block:: bash

   awsqe-client qdel 20260730-141530-a1b2c3                      # one job
   awsqe-client qdel 20260730-141530-a1b2c3 20260730-141602-9f0e11
   awsqe-client qdel 20260730-1415                               # unique prefix
   awsqe-client qdel --queue fast-gpus                           # a whole queue
   awsqe-client qdel --array ffpopt-IDC                          # a whole batch
   awsqe-client qdel --index 2                                   # by position

The selectors cannot be combined in a single command. Nothing is removed unless
every selector resolves, so an unknown or ambiguous job id leaves the queue
untouched. ``qdel`` only removes *queued* jobs; use ``stop`` for one that has
already started.

Deleting a batch
~~~~~~~~~~~~~~~~

``--array NAME`` deletes every queued job tagged into that batch (see
:doc:`tracking-jobs`). Unlike a job id, the name is matched **exactly** — a
prefix would silently widen a destructive operation from one batch to every
batch whose name starts the same way.

Because ``qdel`` reaches only the queue, members that are already running are
reported rather than killed:

.. code-block:: text

   Removed 97 job(s).
     8 member(s) of ffpopt-IDC are already running and were not touched:
     20260802-091500-a1b2c3 on eci5, ... Use `awsqe-client stop <host>` to kill them.

``list --group`` and ``qstat --group`` collapse each batch to one row in the
same way. That is opt-in rather than the default, because those are global
views of everyone's jobs.
