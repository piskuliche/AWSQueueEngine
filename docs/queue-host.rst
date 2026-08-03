Queue host operations
=====================

The queue host owns the queue state and runs the monitor. Install it per
:doc:`installation` first.

Defining queues
---------------

Define your worker queues in a JSON file:

.. code-block:: bash

   cat > /home/ubuntu/awsqueueengine_queues.json <<'JSON'
   {
     "default":   ["eci1", "eci2", "eci3"],
     "fast-gpus": ["eci1", "eci2"],
     "large-mem": ["eci3"]
   }
   JSON

The queue config is the single source of truth for worker assignment. Edit this
file at any time — the monitor reloads it once per poll cycle (~60s), no daemon
restart needed. The journal prints ``[INFO] Queue hosts updated from ...`` when
a change is picked up.

For simple static setups you can skip the file and use one environment variable
instead:

.. code-block:: bash

   AWSQUEUEENGINE_QUEUES="default=eci1,eci2;fast-gpus=eci3"

``AWSQUEUEENGINE_QUEUES_FILE`` and ``AWSQUEUEENGINE_QUEUES`` are mutually
exclusive.

Installing the systemd service
------------------------------

.. code-block:: bash

   # System-wide unit (requires sudo). Writes
   # /etc/systemd/system/awsqe-host.service, daemon-reloads, and enables --now.
   # Runs as $SUDO_USER so the daemon owns the same ~/.awsqe/host/ state files
   # you migrated.
   sudo awsqe-host install
   sudo awsqe-host status
   sudo awsqe-host logs -f       # system journal needs sudo or systemd-journal
                                 # group membership

Per-user variant, if you cannot or would rather not use sudo:

.. code-block:: bash

   awsqe-host install --user
   loginctl enable-linger $USER  # so the daemon survives logout
   awsqe-host logs --user -f

Other daemon verbs: ``start``, ``stop``, ``restart``, ``status``, ``logs``,
``uninstall``. All accept ``--user`` and ``--dry-run``. If systemd is not
available, ``awsqe-host start`` falls back to a foreground run you can Ctrl-C.

.. note::

   Legacy ``awsqueueengine start-monitor`` still works (foreground plus
   pidfile) and will be removed in a later release.

Wiring configuration into the unit
----------------------------------

The systemd service starts with a **clean environment** — it does not read your
``~/.bashrc``. Tell it which queue config and Mailtrap credentials to use via a
drop-in at ``/etc/systemd/system/awsqe-host.service.d/override.conf``.

.. code-block:: bash

   sudo tee /etc/systemd/system/awsqe-host.service.d/override.conf >/dev/null <<'EOF'
   [Service]
   Environment="AWSQUEUEENGINE_QUEUES_FILE=/home/ubuntu/awsqueueengine_queues.json"
   Environment="AWSQUEUEENGINE_MAILTRAP_TOKEN=<your-mailtrap-token>"
   Environment="AWSQUEUEENGINE_MAILTRAP_SENDER_EMAIL=hello@example.com"
   Environment="AWSQUEUEENGINE_MAILTRAP_SENDER_NAME=AWSQueueEngine"
   Environment="AWSQUEUEENGINE_MAILTRAP_CATEGORY=Queue Monitor"
   Environment="AWSQUEUEENGINE_ALERT_TO=you@example.com,team@example.com"
   EOF

   sudo systemctl daemon-reload
   sudo systemctl restart awsqe-host
   sudo systemctl show awsqe-host -p Environment

.. warning::

   **Quote every** ``KEY=VALUE``. systemd splits unquoted values on whitespace
   and silently drops everything after the first space — ``Queue Monitor``
   without quotes parses as ``Queue`` plus a junk ``Monitor`` token that systemd
   warns about and discards.

Keeping the token out of a world-readable file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Put the secret-bearing variables in a separate ``root:root`` mode-600 file and
reference it from the unit:

.. code-block:: bash

   sudo tee /etc/awsqe-host.env >/dev/null <<'EOF'
   AWSQUEUEENGINE_MAILTRAP_TOKEN=<token>
   AWSQUEUEENGINE_MAILTRAP_SENDER_EMAIL=hello@example.com
   AWSQUEUEENGINE_ALERT_TO=you@example.com,team@example.com
   EOF
   sudo chmod 600 /etc/awsqe-host.env

Then in ``override.conf``:

.. code-block:: ini

   [Service]
   Environment="AWSQUEUEENGINE_QUEUES_FILE=/home/ubuntu/awsqueueengine_queues.json"
   EnvironmentFile=/etc/awsqe-host.env

Reading state locally
---------------------

On the queue host itself, ``awsqe-host`` reads the state files directly rather
than going over RPC:

.. code-block:: bash

   awsqe-host list
   awsqe-host qstat
   awsqe-host failed --log
   awsqe-host requeue-running --all
