Tracking your own jobs
======================

``list``, ``qstat`` and ``failed`` are **global** views — the queue host records
no notion of who submitted what, so it cannot answer "how are *my* jobs doing?".
``awsqe-client jobs`` answers that from a local ledger at
``~/.awsqe/client/jobs.json``, appended on every ``awsqe-client submit``,
including submits with no payload directory (which write no ``run.info``
anywhere).

.. note::

   This subcommand is ``awsqe-client``-only. There is no ``awsqueueengine jobs``
   equivalent: the ledger is a purely client-side file, so there is nothing for
   the legacy shim to route to a host.

.. code-block:: text

   $ awsqe-client jobs --since 2d
   SUBMITTED             JOB                     STATUS      HOST      QUEUE         DUR       CMD
   2026-07-30 14:15:30   20260730-141530-a1b2c3  running     eci7      fast-gpus     -         python train.py
   2026-07-30 09:02:11   20260730-090211-9f0e11  failed      eci3      default       01:44:20  bash run.sh
                         -> out_of_memory exit=137 (see `awsqe-client failed --job-id 20260730-090211-9f0e11 --log`)
   2026-07-29 22:10:03   20260729-221003-4c5d6e  queued      -         default       q#3       python sweep.py

Each run refreshes every job that is not already finished, in one round trip per
queue host, and writes the results back. ``--no-refresh`` skips the network
entirely. If a queue host is unreachable the affected jobs keep their last-known
status, a warning goes to stderr, and the rest still refresh — the list is
local, so it stays useful offline.

The ``DUR`` column does double duty: elapsed time for finished jobs, queue
position (``q#3``) for queued ones. Running jobs show ``-``, because the host
reports start times as preformatted text in its own timezone.

Statuses
--------

.. list-table::
   :header-rows: 1
   :widths: 18 82

   * - Status
     - Meaning
   * - ``submitted``
     - Enqueued; not yet refreshed from the queue host.
   * - ``queued``
     - Waiting on the queue host.
   * - ``running``
     - Dispatched to a worker.
   * - ``completed``
     - Finished with exit ``0``.
   * - ``failed``
     - Finished badly; see :doc:`failures`.
   * - ``unknown``
     - Finished, but without a provable exit status.
   * - ``deleted``
     - Removed from the queue by **this client's** ``qdel``.
   * - ``missing``
     - The queue host has no record of it.

``missing`` is the ambiguous one. It means someone else deleted the job, or the
failure aged out of the host's 1000-record history, or the job was submitted to
a different queue host than the one being queried. It is *not* treated as
final — the job keeps getting re-checked, and a whole host's worth of jobs never
flips to ``missing`` at once (that pattern is read as a bad read of the host's
state files rather than as mass deletion).

Filters
-------

.. code-block:: bash

   awsqe-client jobs --status active          # submitted, queued or running
   awsqe-client jobs --status failed --since 7d
   awsqe-client jobs --status queued,running --until 2026-07-30
   awsqe-client jobs --queue gpu,bigmem       # several queues
   awsqe-client jobs -n 10                    # ten most recent (0 for all)
   awsqe-client jobs --forget-before 2026-01-01

* ``--status`` takes any status above, repeated or comma-separated, plus the
  aliases ``active`` (submitted/queued/running), ``done`` and ``all``.
* ``--queue`` filters by queue name, also repeated or comma-separated. Names go
  through the same normalization the queue host applies at submit and are
  matched case-insensitively, so what you can submit to is what you can filter
  on.
* ``--since`` / ``--until`` accept ``YYYY-MM-DD``,
  ``'YYYY-MM-DD HH:MM[:SS]'``, or a relative span (``30m``, ``24h``, ``7d``,
  ``2w``), and filter on submission time in **your** local timezone. As an
  upper bound a bare date means the *end* of that day, so
  ``--until 2026-07-30`` includes the 30th.
* ``--limit`` / ``-n`` caps the rows (default 50; ``0`` for all). A collapsed
  batch is **one row** however many jobs it holds.

Batches
-------

Submitting a folder of work means a shell loop, one ``submit`` per
subdirectory. At a hundred-odd jobs those drown out everything else in
``jobs``. Tag them at submit and they collapse to one row:

.. code-block:: bash

   cd ffpopt
   for IDC in IDC*; do
       awsqe-client submit --queue production --priority -100 --mps \
           --array ffpopt-IDC --payload "${PWD}/$IDC/" \
           "source ~/flowrc && cd \$PAYLOAD_DIR && python run_fe.py"
   done

.. code-block:: text

   $ awsqe-client jobs
   SUBMITTED             ARRAY                       JOBS  QUEUE         STATUS
   2026-08-02 09:14:02   ffpopt-IDC                   142  production    130 completed · 9 failed · 3 running
   2026-08-01 16:40:11   sweep-b                       20  fast-gpus     20 completed

   SUBMITTED             JOB                     STATUS      HOST      QUEUE         DUR       CMD
   2026-08-02 11:02:55   20260802-110255-7ac1e0  running     eci7      default       -         python train.py
   163 of 163 tracked job(s); 162 in 2 batch(es).

* Grouping is **on by default**. Untagged jobs list exactly as they always have,
  below the batch rows.
* ``SUBMITTED`` on a batch row is when you fired it off — the *earliest* of its
  jobs — but batches sort by their most recent, so one still being submitted
  stays at the top.
* ``--array NAME`` drills into one batch and lists its jobs individually;
  ``--expand`` (or ``--no-group``) turns grouping off entirely. ``--fetch-logs``
  implies expansion, since a log path has nowhere to go on a batch row.
* The other filters compose, and the batch row reflects what survived them:
  ``jobs --array ffpopt-IDC --status failed --fetch-logs`` is the practical way
  to read the tracebacks out of a batch.
* A batch that went to more than one queue shows ``*`` in the ``QUEUE`` column
  rather than picking one of them.

Names are letters, digits, ``.``, ``_`` and ``-``, up to 64 characters, with no
punctuation at either end. An unusable name is **rejected** rather than quietly
rewritten — unlike a queue name, an array name is something you type back in at
``jobs --array``, and a silent rewrite at submit would leave that matching
nothing with nothing on screen to explain why.

Reusing a name is legal and appends to that batch; ``--since`` separates the
runs.

Job logs
--------

``--fetch-logs`` copies each displayed job's log off the worker that ran it into
``~/.awsqe/client/logs/<job_id>.log``, and prints the local path under the row:

.. code-block:: text

   2026-08-01 10:51:39   20260731-181013-cfd2e8  completed   eci3      production    00:00:06  python run_fe.py
                         log: /home/you/.awsqe/client/logs/20260731-181013-cfd2e8.log

To read one job's log, ``--cat`` prints it straight to the screen and ``--log``
prints just its path — both fetch first if the log is not cached yet:

.. code-block:: bash

   awsqe-client jobs --cat 20260731-1810              # the log itself
   awsqe-client jobs --cat 20260731-1810 | grep Error # nothing else goes to stdout
   less "$(awsqe-client jobs --log 20260731-1810)"    # the path, for other tools

Fetching goes **client to worker** over ``scp``, not through the queue host, so
it needs SSH access to the workers (the same access ``tail``, ``stop`` and
``status`` already assume). It is opt-in because it costs one connection per
job, and it is scoped to the rows actually displayed — ``-n 5 --fetch-logs``
fetches five logs, not your whole history.

Already-fetched logs are skipped, but the cache is keyed on **which worker and
which finish time**, not just the job id. A requeued job truncates its log and
may land on a different worker, so a rerun re-fetches rather than serving the
previous attempt. Running jobs are always re-fetched, since their log is still
being written. When a worker no longer has the log — recycled, or
``manager_jobs`` cleaned — that is recorded so it is not retried every run:

.. code-block:: text

   [WARN] 19990101-000000-deadbe: no log left on eci3 (worker recycled or cleaned)

The cache is capped at 512 MB, dropping the oldest first, and ``--forget``
deletes a job's cached log along with its ledger entry.

Ledger housekeeping
-------------------

The ledger holds 2000 jobs, dropping the oldest *finished* ones past that —
jobs still in flight are never evicted. ``--forget <job-id-or-prefix>`` and
``--forget-before <when>`` remove entries by hand; both only stop tracking, they
never cancel anything (use ``qdel`` for that).

``awsqe-client info --job-id <id-or-prefix>`` refreshes a single tracked job
without needing to ``cd`` to its payload directory, and rewrites that payload's
``run.info`` if it still exists. It is also how to re-check a job the list
already considers finished, since ``jobs`` does not re-query those.

Two caveats worth knowing: the ledger is **per machine**, so submitting from a
laptop and a workstation gives each its own half of the picture; and a queue
host running an older ``awsqe-host`` has no batched lookup, so the refresh falls
back to one round trip per job (it says so, once, on stderr).
