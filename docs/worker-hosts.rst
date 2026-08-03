Worker hosts
============

Worker hosts do not need AWSQueueEngine installed. They need to be reachable by
SSH from the queue host, and to have scratch space under the configured scratch
roots.

The queue host launches jobs over SSH; workers download payload archives
directly from S3 and extract them before running the command with
``PAYLOAD_DIR`` set.

S3 access
---------

For S3-backed payloads, workers also need the AWS CLI and IAM permissions to
**read** the payload bucket and prefix:

.. code-block:: bash

   aws s3 ls s3://my-queue-payload-bucket/awsqueueengine/payloads/

That is read access only — uploads happen from the submitter, not the worker.

Checking on workers
-------------------

These commands SSH to the worker directly from your machine, so they need your
own SSH access to the workers, not just to the queue host:

.. code-block:: bash

   awsqe-client status       # ps probe of every host's MANAGER_TAG state
   awsqe-client where        # probe scratch space on every worker
   awsqe-client tail eci17   # tail the most recent job log
   awsqe-client stop eci17   # kill managed job(s) on a worker

``awsqe-client jobs --fetch-logs`` uses the same path — see
:doc:`tracking-jobs`.

The one exception is the ``tail`` RPC method, which has the *queue host* reach
the worker on your behalf; that works from a client with no direct route to the
workers. See :doc:`protocol`.

Job logs on the worker
----------------------

Managed jobs write to ``~/manager_jobs/`` on the worker: ``<job_id>.log`` for
output and ``<job_id>.rc`` for the exit status the monitor reads back. Cleaning
that directory is safe once jobs have finished and their logs have been
fetched, but it means ``--fetch-logs`` can no longer retrieve them — see
:doc:`failures`.
