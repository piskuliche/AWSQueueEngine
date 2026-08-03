Command reference
=================

Three entry points are installed. ``awsqe-client`` and ``awsqe-host`` are the
supported ones; ``awsqueueengine`` is a backward-compatibility shim.

Run any of them with ``--help``, or a subcommand with ``--help``, for the
current flags. This page is the map, not an exhaustive flag list.

``awsqe-client``
----------------

Runs on your machine. Talks to the queue host over RPC, except where noted.

.. list-table::
   :header-rows: 1
   :widths: 24 76

   * - Subcommand
     - What it does
   * - ``submit``
     - Enqueue a job. See :doc:`submitting`.
   * - ``list``
     - Queued jobs on the queue host (global).
   * - ``qstat``
     - Running jobs, with elapsed time (global).
   * - ``qdel``
     - Delete queued jobs by id, prefix, queue, or position.
   * - ``failed``
     - Failure records, newest first. See :doc:`failures`.
   * - ``deferred``
     - Jobs parked past the submit-failure limit.
   * - ``requeue-deferred``
     - Put deferred jobs back on the queue, or drop them.
   * - ``enable-host``
     - List host cooldowns, or clear them.
   * - ``jobs``
     - **Your** submissions, from the local ledger. See :doc:`tracking-jobs`.
   * - ``info``
     - Refresh one job and rewrite its ``run.info``.
   * - ``config``
     - Get, set, unset and show client settings. See :doc:`configuration`.
   * - ``status``
     - Probe every worker's managed-job state (SSHes to workers).
   * - ``tail``
     - Tail the most recent job log on a worker (SSHes to the worker).
   * - ``stop``
     - Kill managed job(s) on a worker (SSHes to the worker).
   * - ``where``
     - Probe scratch space on every worker (SSHes to workers).

``awsqe-host``
--------------

Runs on the queue host and reads its state files directly.

.. list-table::
   :header-rows: 1
   :widths: 24 76

   * - Subcommand
     - What it does
   * - ``list``, ``qstat``, ``qdel``, ``failed``, ``deferred``,
       ``requeue-deferred``, ``enable-host``, ``submit``, ``status``, ``stop``
     - Local equivalents of the client subcommands above.
   * - ``requeue-running``
     - Kill and requeue running jobs at priority 100.
   * - ``clear``
     - Clear queue state.
   * - ``job-info``
     - Look up one job.
   * - ``monitor``
     - Run the monitor loop in the foreground.
   * - ``rpc``
     - Serve one RPC request on stdin/stdout. See :doc:`protocol`.
   * - ``migrate``
     - Migrate legacy state files. See :doc:`migration`.
   * - ``install``, ``uninstall``, ``start``, ``stop``, ``restart``,
       ``status``, ``logs``
     - systemd service management. See :doc:`queue-host`.

``awsqueueengine`` (legacy)
---------------------------

The original single-binary CLI, kept so older scripts keep working. It builds a
union parser and dispatches each subcommand to the client or host handler,
routing to the configured queue host where one applies.

It accepts: ``submit``, ``list``, ``qstat``, ``qdel``, ``clear``, ``deferred``,
``requeue-deferred``, ``requeue-running``, ``enable-host``, ``status``,
``stop``, ``tail``, ``where``, ``info``, ``job-info``, ``start``,
``start-monitor``, ``stop-monitor``, ``status-monitor``.

.. note::

   Three things it does **not** have, because they are purely client-side and
   there is nothing to route to a host: ``jobs``, ``failed`` and ``config``.
   Use ``awsqe-client`` for those.

   It does not currently emit a ``DeprecationWarning``; a future release will
   add one. New scripts should use ``awsqe-client`` and ``awsqe-host``.
