Architecture
============

AWSQueueEngine is a small, Slurm-inspired job manager for a fixed set of
SSH-accessible hosts. It is two binaries that talk over a JSON-over-SSH RPC:

``awsqe-client``
   Runs on your laptop or dev box. Archives payloads, uploads them to S3, and
   asks the queue host to enqueue jobs. Keeps a local ledger of what *you*
   submitted.

``awsqe-host``
   Runs on the queue-manager VM as a systemd service. Owns the queue state,
   dispatches jobs to worker hosts over SSH, tracks and preempts them, and sends
   email alerts.

The legacy ``awsqueueengine`` command remains as a backward-compatibility shim
that dispatches to one of the two based on the subcommand.

What it does
------------

* Queues jobs with integer priorities.
* Submits to *user queues* whose concrete worker hosts are resolved at dispatch
  time, so hosts can move between queues without touching queued work.
* Uploads payload directories to S3 and stages them onto worker scratch space.
* Monitors running jobs, records completion metadata, and classifies failures.
* Preempts lower-priority jobs and requeues them to their original host.
* Benches hosts that fail repeatedly, and alerts by email.

The three roles
---------------

Local submitter
   Your laptop or workstation. Runs ``awsqe-client``. Needs SSH to the queue
   host, and — for ``--fetch-logs``, ``tail``, ``stop`` and ``status`` — direct
   SSH to the workers as well.

Queue host
   One machine that owns the queue files and runs the monitor. Everything
   authoritative lives here. See :doc:`queue-host`.

Worker hosts
   The machines where jobs actually execute. Reached over SSH from the queue
   host. See :doc:`worker-hosts`.

Package layout
--------------

The split between the two sides is enforced in the source tree:

``src/awsqueueengine/shared/``
   Data models, the wire protocol, path constants — used by both sides.

``src/awsqueueengine/client/``
   The ``awsqe-client`` CLI, submit, ``run.info``, the tracked-job ledger, RPC
   transport.

``src/awsqueueengine/host/``
   The ``awsqe-host`` CLI, monitor loop, job control, migration, daemon.

``src/awsqueueengine/cli.py``
   The legacy shim that dispatches to one of the two.

**Code in** ``client/`` **and** ``host/`` **must never import each other.**
``shared/`` is the only bridge; ``cli.py`` is the one sanctioned exception. See
:doc:`api/index`.

Also in the repository: ``scripts/`` (local-only smoke tests for remote queue
host validation), ``tests/`` (unit and subprocess tests), and ``setup.py``.

State on disk
-------------

The queue host keeps its state under ``~/.awsqe/host/``:

.. list-table::
   :header-rows: 1
   :widths: 34 66

   * - File
     - Contents
   * - ``queue.json``
     - Jobs waiting to be dispatched.
   * - ``running.json``
     - Jobs currently on a worker, keyed by host.
   * - ``completed.json``
     - Finished jobs, with duration and timestamps.
   * - ``failed.json``
     - Failure records; capped at the 1000 most recent.
   * - ``deferred.json``
     - Jobs parked after repeated submit failures.
   * - ``monitor_state.json``
     - Monitor bookkeeping, including host cooldowns.
   * - ``lock``, ``pid``
     - Monitor single-instance guards.

The client keeps its own state under ``~/.awsqe/client/``: ``config.toml``
(:doc:`configuration`), ``jobs.json`` (the tracked-job ledger) and ``logs/``
(the fetched-log cache).

.. note::

   These paths replaced an older ``~/.aws_slurm_like_*.json`` layout. If you are
   upgrading from that, see :doc:`migration`.
