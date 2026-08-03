SSH JSON-RPC Protocol
=====================

This is the contract between ``awsqe-client`` and ``awsqe-host``. Any client
that can open an SSH connection and speak JSON can drive the queue — the
Android viewer does exactly that.

.. automodule:: awsqueueengine.shared.protocol
   :no-members:

Methods
-------

A method exists if and only if it is a key in
:data:`~awsqueueengine.host.rpc.METHODS`. Each links to its handler, whose
docstring carries the full parameter and result detail.

.. list-table::
   :header-rows: 1
   :widths: 18 32 32 18

   * - Method
     - Params
     - Result
     - Errors
   * - ``enqueue``
     - ``cmd``\*, ``queue``, ``hosts``, ``priority``, ``high_priority``,
       ``preempt``, ``mps``, ``payload``, ``payload_s3_uri``,
       ``payload_size_bytes``, ``job_id``
     - ``job_id``, ``queue``, ``hosts``
     - ``invalid_params``, ``internal``

       :func:`~awsqueueengine.host.rpc.handle_enqueue`
   * - ``enqueue_many``
     - ``items``\* (list of ``enqueue`` param objects)
     - ``enqueued``, per-item results
     - ``invalid_params``, ``internal``

       :func:`~awsqueueengine.host.rpc.handle_enqueue_many`
   * - ``list``
     - —
     - ``jobs`` (queue order)
     - :func:`~awsqueueengine.host.rpc.handle_list`
   * - ``qstat``
     - —
     - ``running`` (keyed by host)
     - :func:`~awsqueueengine.host.rpc.handle_qstat`
   * - ``qdel``
     - ``job_ids``, ``indices``, ``queue``, ``array_id``
     - ``removed``, plus ``running`` for an ``array_id`` selector
     - ``invalid_params``, ``not_found``, ``conflict``

       :func:`~awsqueueengine.host.rpc.handle_qdel`
   * - ``deferred_list``
     - —
     - ``jobs``
     - :func:`~awsqueueengine.host.rpc.handle_deferred_list`
   * - ``failed_list``
     - ``limit``, ``log``, ``job_id``
     - ``jobs``
     - ``invalid_params``

       :func:`~awsqueueengine.host.rpc.handle_failed_list`
   * - ``requeue_deferred``
     - ``indices`` XOR ``all``, ``drop``
     - ``moved``, ``action``
     - ``invalid_params``, ``conflict``

       :func:`~awsqueueengine.host.rpc.handle_requeue_deferred`
   * - ``list_cooldowns``
     - —
     - ``cooldowns``
     - :func:`~awsqueueengine.host.rpc.handle_list_cooldowns`
   * - ``enable_host``
     - ``hosts`` XOR ``all``
     - ``cleared``
     - ``invalid_params``

       :func:`~awsqueueengine.host.rpc.handle_enable_host`
   * - ``job_info``
     - ``job_id``\*
     - ``state``
     - ``invalid_params``

       :func:`~awsqueueengine.host.rpc.handle_job_info`
   * - ``job_info_batch``
     - ``job_ids``\*
     - ``states``, ``skipped``
     - ``invalid_params``

       :func:`~awsqueueengine.host.rpc.handle_job_info_batch`
   * - ``tail``
     - ``host``\*, ``lines``
     - tail payload
     - ``invalid_params``

       :func:`~awsqueueengine.host.rpc.handle_tail`
   * - ``stats``
     - —
     - counters plus the underlying name lists
     - :func:`~awsqueueengine.host.rpc.handle_stats`

\* required. The handlers are documented in full under
:doc:`api/host`.

Envelope helpers
----------------

.. autofunction:: awsqueueengine.shared.protocol.make_request
.. autofunction:: awsqueueengine.shared.protocol.make_ok
.. autofunction:: awsqueueengine.shared.protocol.make_error

.. autoexception:: awsqueueengine.shared.protocol.RpcError
.. autoexception:: awsqueueengine.shared.protocol.RpcTransportError

Calling the protocol from Python
--------------------------------

.. automodule:: awsqueueengine.shared.rpc_client
