Shared
======

Used by both sides. This is the only package ``client/*`` and ``host/*`` are
allowed to import from.

Paths and state
---------------

.. automodule:: awsqueueengine.shared.paths
   :undoc-members:

.. automodule:: awsqueueengine.shared.queue

.. automodule:: awsqueueengine.shared.running_state

.. automodule:: awsqueueengine.shared.completion_state

.. automodule:: awsqueueengine.shared.deferred_state

.. automodule:: awsqueueengine.shared.failure_state

.. automodule:: awsqueueengine.shared.host_status

Queues and configuration
------------------------

The constants in :mod:`~awsqueueengine.shared.config` are read from the
environment at import time; as on the host side, the environment variables are
the contract and the values are not rendered.

.. automodule:: awsqueueengine.shared.config

.. automodule:: awsqueueengine.shared.queue_config

Jobs and identity
-----------------

.. automodule:: awsqueueengine.shared.array_id

.. automodule:: awsqueueengine.shared.job_status

.. automodule:: awsqueueengine.shared.job_lookup

.. automodule:: awsqueueengine.shared.job_outcome

.. automodule:: awsqueueengine.shared.run_info

.. automodule:: awsqueueengine.shared.timespec

Transport and workers
---------------------

.. automodule:: awsqueueengine.shared.protocol

.. automodule:: awsqueueengine.shared.rpc_client

.. automodule:: awsqueueengine.shared.ssh_utils

.. automodule:: awsqueueengine.shared.worker_actions

.. automodule:: awsqueueengine.shared.worker_staging

CLI helpers
-----------

.. automodule:: awsqueueengine.shared.cli_utils
