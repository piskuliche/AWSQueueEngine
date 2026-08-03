Configuration
=============

Client configuration
--------------------

Configure the client once so you do not have to pass ``--queue-host`` or S3
flags on every command. Settings live in ``~/.awsqe/client/config.toml``:

.. code-block:: bash

   awsqe-client config set queue-host queue-manager
   awsqe-client config set s3.bucket  amberflow-default
   awsqe-client config set s3.prefix  jobs

   awsqe-client config show              # inspect what is set
   awsqe-client config get queue-host    # one key
   awsqe-client config unset queue-host  # clear one key

There are exactly three keys:

.. list-table::
   :header-rows: 1
   :widths: 18 34 48

   * - Key
     - Resolution order
     - Meaning
   * - ``queue-host``
     - ``--queue-host`` flag, then config
     - SSH destination of the queue host. **No environment variable** — it is
       a flag or the config file, and a command that needs it and finds
       neither fails rather than guessing.
   * - ``s3.bucket``
     - ``AWSQUEUEENGINE_S3_BUCKET``, then config
     - Bucket for uploaded payload archives.
   * - ``s3.prefix``
     - ``AWSQUEUEENGINE_S3_PREFIX``, then config
     - Key prefix within the bucket. Optional.

Note that the two S3 settings take the **environment variable first**, which
makes them convenient to override for a one-off run or in CI, while
``queue-host`` deliberately has no such override.

The legacy ``awsqueueengine`` CLI reads the same config, so ``awsqueueengine
list`` with no flag routes to the configured queue host. To force a local read
on the queue host itself, use ``awsqe-host list`` directly.

For S3-backed payload submit you also need AWS credentials with write access to
the bucket, configured the usual way (``~/.aws/credentials``, environment
variables, or an IAM role).

Queue host configuration
------------------------

The queue host is configured entirely through environment variables — it runs
as a systemd service, so the values are wired into the unit rather than a shell
profile. See :doc:`queue-host` for how to set them.

.. important::

   Every one of these is read **at import time**, so a change needs a restart:
   ``sudo systemctl restart awsqe-host``.

Queue definitions
~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Variable
     - Meaning
   * - ``AWSQUEUEENGINE_QUEUES_FILE``
     - Path to a JSON file mapping queue names to host lists. Reloaded once
       per poll cycle, so edits take effect without a restart.
   * - ``AWSQUEUEENGINE_QUEUES``
     - Inline form, e.g. ``default=eci1,eci2;fast-gpus=eci3``. Static; read
       once at process start.
   * - ``AWSQUEUEENGINE_HOSTS_FILE``
     - A flat host list, for setups predating queues.

``AWSQUEUEENGINE_QUEUES_FILE`` and ``AWSQUEUEENGINE_QUEUES`` are **mutually
exclusive**. Prefer the file — it is the only form you can edit without
restarting the daemon.

Failure handling
~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 52 12 36

   * - Variable
     - Default
     - Meaning
   * - ``AWSQUEUEENGINE_MAX_SUBMIT_FAILURES``
     - ``3``
     - Submit attempts before a job moves to ``deferred.json``.
   * - ``AWSQUEUEENGINE_HOST_STORAGE_COOLDOWN_SECONDS``
     - ``7200``
     - How long a host is benched after a storage failure.
   * - ``AWSQUEUEENGINE_HOST_TRANSPORT_COOLDOWN_SECONDS``
     - ``600``
     - How long a host is benched after a transport failure.

Email alerts are configured through a further set of variables — see
:doc:`alerts`.

.. note::

   The monitor's poll interval is a constant (60 seconds), not an environment
   variable.

Transport
~~~~~~~~~

``AWSQUEUEENGINE_SCP_BIN`` overrides the ``scp`` binary used to fetch job logs,
which is occasionally useful when the one on ``PATH`` is a wrapper.
