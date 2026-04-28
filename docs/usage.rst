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
   export AWSQUEUEENGINE_HOSTS_FILE="/home/ubuntu/queue_hosts.txt"
   nohup awsqueueengine start-monitor >> ~/aws_queue_manager.log 2>&1 &

The hosts file should list every worker the monitor is allowed to use. You can
also define named host sets on the queue host for user-friendly targeting:

.. code-block:: bash

   export AWSQUEUEENGINE_HOST_SET_FAST_GPUS="eci1 eci2 eci3"
   export AWSQUEUEENGINE_HOSTS_FILE_LARGE_MEM="/home/ubuntu/large_mem_hosts.txt"

Users can then submit to those pools without knowing the individual host names:

.. code-block:: bash

   awsqueueengine submit --queue-host queue-manager --host-set fast-gpus "python train.py"

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
   awsqueueengine submit --queue-host queue-manager --host-set fast-gpus "python train.py"
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

Worker hosts need the AWS CLI and read access to the same bucket/prefix. Host
validation for remote submit happens on the queue host; set
``AWSQUEUEENGINE_HOSTS_FILE=/path/to/queue_hosts.txt`` there to use a central
host list. Configure an S3 lifecycle rule for uploaded payload archives.

Named host sets can be configured on the queue host and selected during submit:

.. code-block:: bash

   export AWSQUEUEENGINE_HOST_SET_FAST_GPUS="eci1 eci2 eci3"
   export AWSQUEUEENGINE_HOSTS_FILE_LARGE_MEM="/home/ubuntu/large_mem_hosts.txt"
   awsqueueengine submit --queue-host queue-manager --host-set fast-gpus "python train.py"

``fast-gpus`` maps to ``AWSQUEUEENGINE_HOST_SET_FAST_GPUS`` or
``AWSQUEUEENGINE_HOSTS_FILE_FAST_GPUS``. The monitor still runs once over the
full host pool; the host set is stored as the job's host allowlist.

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
