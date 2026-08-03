Legacy shim
===========

.. note::

   ``awsqueueengine`` is the original single-binary entry point, kept so older
   scripts keep working. It builds a union argument parser and dispatches each
   subcommand to the client or host handler. New work should use
   ``awsqe-client`` and ``awsqe-host``, which are the supported entry points.

   Invoking it does **not** currently emit a ``DeprecationWarning``; a future
   release will add one.

This is also the one module permitted to import from both
:mod:`awsqueueengine.client` and :mod:`awsqueueengine.host`.

.. automodule:: awsqueueengine.cli
