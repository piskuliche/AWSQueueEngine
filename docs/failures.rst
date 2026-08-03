Failures
========

Failed jobs
-----------

Every job is launched wrapped so the worker writes its exit status to
``~/manager_jobs/<job_id>.rc`` next to the job log. When the monitor sees a host
go idle it reads that status back: a clean ``0`` becomes a completion record,
and anything else becomes a **failure record** in ``~/.awsqe/host/failed.json``
— including jobs that die seconds after launch, which previously left no trace
at all.

Each failure record carries the usual job fields plus ``exit_code``, a short
``failure_reason`` slug, a one-line ``failure_detail``, and the last 40 lines of
the job log (``log_tail``):

.. code-block:: bash

   awsqe-client failed                      # 50 most recent, newest first
   awsqe-client failed -n 200               # more history
   awsqe-client failed --job-id JOB --log   # one job, with its captured log tail
   awsqe-host failed --log                  # same view, on the queue host

``awsqe-client info -p ./my_payload`` reports ``status: failed`` for a failed
job and writes ``failure_reason``, ``failure_detail`` and ``exit_code`` into the
payload's ``run.info``.

The failure history is capped at the 1000 most recent records.

Failure reasons
~~~~~~~~~~~~~~~

A rough classification, taken from the log tail first and the exit status
second: ``out_of_memory``, ``disk_full``, ``cuda_error``,
``command_not_found``, ``permission_denied``, ``python_import_error``,
``python_exception``, ``not_executable``, ``killed``, ``segfault``,
``terminated``, ``signal_N``, ``nonzero_exit``.

Two more describe *how* the job ended rather than why:

``start_failed``
   The job never ran — a submit failure. Repeated start failures still move the
   job to ``deferred.json`` for requeueing.

``no_exit_status``
   The job vanished without recording a status. Usually a hard kill, a host
   reboot, or an operator running ``stop`` / ``requeue-running`` against the
   host. The failure history records those rather than counting them as clean
   finishes.

A job that exits before the monitor can confirm its pid is judged by its
recorded status, not by its absence: exit ``0`` there means the work was already
done and the job is filed as **completed** (no alert, and the host stays
available for the next queued job), while a nonzero or missing status is filed
as a failure.

.. note::

   This only applies to jobs *this* monitor launched, which it marks with
   ``exit_status_tracked`` in ``running.json``. Jobs already running when the
   daemon is upgraded never got the wrapper, so no status file exists for them;
   those finish as ``status: unknown`` in ``completed.json``, never as failures
   — a clean multi-hour run must not be reported as broken just because its
   outcome cannot be proven. The distinction only matters for jobs in flight
   across a single upgrade.

.. _exit-status-trap:

Your command's exit status is what gets recorded
------------------------------------------------

.. warning::

   The status written to ``<job_id>.rc`` is the **shell's** exit status, which
   with ``;``-chained commands is the status of the *last* one. A trailing
   cleanup step therefore hides whatever went wrong before it.

.. code-block:: bash

   # python fails (exit 1), rm succeeds (exit 0) -> the shell exits 0
   # -> recorded as COMPLETED, no failure record, no alert
   cd $PAYLOAD_DIR; python run_fe.py; rm -rf $PAYLOAD_DIR

Chain with ``&&`` instead. The status is then the first failure's, and the
payload directory survives for you to inspect:

.. code-block:: bash

   cd $PAYLOAD_DIR && python run_fe.py && rm -rf $PAYLOAD_DIR

If cleanup has to happen either way, capture the status first:

.. code-block:: bash

   cd $PAYLOAD_DIR; python run_fe.py; rc=$?; rm -rf $PAYLOAD_DIR; exit $rc

Nothing on the queue host can detect this — a shell that exits 0 finished
successfully as far as any caller can tell. ``awsqe-client jobs --fetch-logs``
is the practical check: pull the logs and look for tracebacks in jobs that claim
to have completed.

Deferred jobs
-------------

A job that fails to *start* repeatedly is moved out of the queue into
``~/.awsqe/host/deferred.json`` rather than retried forever. The threshold is
``AWSQUEUEENGINE_MAX_SUBMIT_FAILURES`` (default 3).

.. code-block:: bash

   awsqe-client deferred                    # what is parked
   awsqe-client requeue-deferred --all      # put it all back
   awsqe-client requeue-deferred --index 2  # or one entry

Requeued jobs get their submit-failure count reset, so they get a full set of
fresh attempts.

Host cooldowns
--------------

A host that fails with a storage or transport error is benched for a while
rather than being handed more work: ``AWSQUEUEENGINE_HOST_STORAGE_COOLDOWN_SECONDS``
(default 7200) and ``AWSQUEUEENGINE_HOST_TRANSPORT_COOLDOWN_SECONDS``
(default 600).

.. code-block:: bash

   awsqe-client enable-host          # list active cooldowns
   awsqe-client enable-host eci17    # release one host early
   awsqe-client enable-host --all    # release everything

Naming a host that is not on cooldown is a no-op, not an error.
