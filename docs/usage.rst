Usage Guide
===========

Installation
------------

From the project root:

.. code-block:: bash

   pip install .

Basic Commands
--------------

.. code-block:: bash

   awsqueueengine status
   awsqueueengine submit --payload ./my_payload "cd $PAYLOAD_DIR && bash run.sh"
   awsqueueengine submit --priority 25 "python train.py --epochs 10"
   awsqueueengine submit --hosts eci17 --priority 100 "bash pinned-job.sh"
   awsqueueengine submit --preempt --priority 999 "bash urgent-job.sh"
   awsqueueengine requeue-running --hosts eci17
   awsqueueengine requeue-running --all
   awsqueueengine list
   awsqueueengine qstat
   awsqueueengine qdel 2
   awsqueueengine start-monitor
   awsqueueengine stop-monitor

Host Targeting
--------------

Use ``--hosts`` multiple times or pass a comma-separated list to constrain a job to one or more hosts.

.. code-block:: bash

   awsqueueengine submit --hosts eci16 --hosts eci18 "bash pinned-job.sh"
   awsqueueengine requeue-running --hosts eci16,eci18

You can also manage the monitored host pool from a file:

.. code-block:: bash

   awsqueueengine status --hosts-file ~/queue_hosts.txt
   awsqueueengine start-monitor --hosts-file ~/queue_hosts.txt

Payloads
--------

When a payload directory is supplied, the engine stages it to remote scratch space and runs the
job with ``PAYLOAD_DIR`` set to the remote payload path.

.. code-block:: bash

   awsqueueengine submit --payload ./my_payload "cd $PAYLOAD_DIR && bash run.sh"

If a running job is requeued or preempted, the queue engine preserves the remote payload path so
the restarted job still receives the correct ``PAYLOAD_DIR``.

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
