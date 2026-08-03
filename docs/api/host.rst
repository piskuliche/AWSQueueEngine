Host
====

Runs on the queue host. Nothing here may import from
:mod:`awsqueueengine.client`.

Command line
------------

.. automodule:: awsqueueengine.host.cli

Monitor loop
------------

.. automodule:: awsqueueengine.host.monitor

.. automodule:: awsqueueengine.host.job_control

RPC server
----------

The handlers below implement the methods listed in the protocol reference.

.. automodule:: awsqueueengine.host.rpc

Service management
------------------

.. automodule:: awsqueueengine.host.daemon

.. automodule:: awsqueueengine.host.migration

Notifications
-------------

.. automodule:: awsqueueengine.host.notifications

Configuration
-------------

This module is a flat set of constants read from ``AWSQUEUEENGINE_*``
environment variables at import time. Their values are deliberately not
rendered here — the environment variables are the contract, not the constants.

.. automodule:: awsqueueengine.host.config
