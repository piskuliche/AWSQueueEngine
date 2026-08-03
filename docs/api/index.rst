API Reference
=============

The package is split three ways, and the split is load-bearing:

``awsqueueengine.client``
   Runs on the submitter's machine. Talks to the queue host over SSH JSON-RPC
   and owns purely local state — the config file, the tracked-job ledger, the
   log cache.

``awsqueueengine.host``
   Runs on the queue host. Owns the queue and the monitor loop, and is the only
   side that touches worker machines.

``awsqueueengine.shared``
   Everything both sides need: the wire protocol, state file schemas, path
   constants, and small helpers.

**Code in** ``client/*`` **and** ``host/*`` **must never import each other.**
``shared/*`` is the only bridge between them. :mod:`awsqueueengine.cli`, the
legacy shim, is the single sanctioned exception — it imports both in order to
dispatch the old union CLI.

.. toctree::
   :maxdepth: 2

   client
   host
   shared
   legacy
