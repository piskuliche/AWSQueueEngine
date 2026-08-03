State migration
===============

The queue host's state files moved from ``~/.aws_slurm_like_*.json`` to
``~/.awsqe/host/``:

.. code-block:: text

   ~/.awsqe/host/queue.json
   ~/.awsqe/host/running.json
   ~/.awsqe/host/completed.json
   ~/.awsqe/host/failed.json      # no legacy counterpart; created on first failure
   ~/.awsqe/host/deferred.json
   ~/.awsqe/host/monitor_state.json
   ~/.awsqe/host/lock
   ~/.awsqe/host/pid

The daemon migrates them on first start. You can also run it explicitly:

.. code-block:: bash

   awsqe-host migrate --dry-run     # preview what would move
   awsqe-host migrate               # actually move (idempotent)
   awsqe-host migrate --force       # re-run even if already migrated

For each legacy file the migration:

1. Copies it, preserving mtime and permissions, to its new home in
   ``~/.awsqe/host/``.
2. Renames the legacy file to ``~/.aws_slurm_like_*.json.migrated.bak``.
3. Stamps ``migrated_at`` in the new ``monitor_state.json``, so subsequent runs
   are a no-op.

The original files are renamed rather than deleted, which is what makes the
rollback below possible.

Rolling back
------------

Move the ``.migrated.bak`` files back to their original names and remove the new
directory:

.. code-block:: bash

   for f in ~/.aws_slurm_like_*.migrated.bak; do mv "$f" "${f%.migrated.bak}"; done
   rm -rf ~/.awsqe/host

.. warning::

   Stop the daemon first (``sudo awsqe-host stop``). Rolling back underneath a
   running monitor loses whatever it writes in the meantime.
