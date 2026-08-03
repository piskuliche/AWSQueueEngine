AWSQueueEngine
==============

A Slurm-like SSH job manager for AWS GPU hosts, with payload staging and
monitoring.

The codebase is two binaries that talk over a JSON-over-SSH RPC:

* **awsqe-client** — runs on your laptop or dev box; archives payloads, uploads
  them to S3, and asks the queue host to enqueue jobs.
* **awsqe-host** — runs on the queue-manager VM as a systemd service; owns the
  queue state, dispatches jobs to worker hosts over SSH, and sends email alerts.

The legacy ``awsqueueengine`` command remains as a backward-compatibility shim
that dispatches to one of the two based on the subcommand.

Requires **Python 3.10+** on both the local submitter and the queue host.

Start here
----------

* New to it? :doc:`installation`, then :doc:`configuration`, then
  :doc:`quickstart`.
* Setting up the queue host? :doc:`queue-host` and :doc:`worker-hosts`.
* Something failed? :doc:`failures` — and note the
  :ref:`exit-status trap <exit-status-trap>`.
* Writing another client? :doc:`protocol`.

.. toctree::
   :maxdepth: 2
   :caption: Getting started

   installation
   configuration
   quickstart

.. toctree::
   :maxdepth: 2
   :caption: User guide

   submitting
   tracking-jobs
   failures

.. toctree::
   :maxdepth: 2
   :caption: Operations

   queue-host
   worker-hosts
   migration
   alerts

.. toctree::
   :maxdepth: 2
   :caption: Reference

   cli
   protocol
   architecture
   api/index

.. toctree::
   :maxdepth: 1
   :caption: Project

   contributing
