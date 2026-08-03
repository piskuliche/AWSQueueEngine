Installation
============

Requires **Python 3.10+** on both the local submitter and the queue host.

Installing gives you three entry points: ``awsqe-client``, ``awsqe-host``, and
the legacy ``awsqueueengine`` shim.

Local submitter
---------------

A ``pip install --user`` is fine here. On Ubuntu 23.04+ you may need the PEP 668
escape hatch:

.. code-block:: bash

   git clone <repo-url> AWSQueueEngine
   cd AWSQueueEngine
   python3 -m pip install --user -e .                          # most systems
   # or, on Ubuntu 23.04+:
   python3 -m pip install --user --break-system-packages -e .

The three commands land in ``~/.local/bin``. Make sure that is on ``$PATH``.

Then point the client at your queue host once — see :doc:`configuration`.

Queue host
----------

Use a **dedicated venv** so the daemon's dependencies do not fight
apt-installed system packages (PEP 668), and **symlink the binaries into**
``/usr/local/bin`` so non-interactive SSH from clients can find ``awsqe-host``
on ``PATH``. That second step is not optional: the RPC transport runs
``ssh <queue_host> awsqe-host rpc``, which gets a login shell with a minimal
environment.

.. code-block:: bash

   sudo apt-get install -y python3 python3-venv python3-pip git

   git clone <repo-url> ~/AWSQueueEngine
   cd ~/AWSQueueEngine

   sudo python3 -m venv /opt/awsqueueengine-venv
   sudo /opt/awsqueueengine-venv/bin/pip install -U pip
   sudo /opt/awsqueueengine-venv/bin/pip install -e .

   sudo ln -sf /opt/awsqueueengine-venv/bin/awsqe-host     /usr/local/bin/awsqe-host
   sudo ln -sf /opt/awsqueueengine-venv/bin/awsqe-client   /usr/local/bin/awsqe-client
   sudo ln -sf /opt/awsqueueengine-venv/bin/awsqueueengine /usr/local/bin/awsqueueengine

Verify:

.. code-block:: bash

   which awsqe-host && head -1 $(which awsqe-host)
   # Should show /usr/local/bin/awsqe-host with a shebang pointing at
   # /opt/awsqueueengine-venv/bin/python3

Next: define your queues and install the service — see :doc:`queue-host`.

To remove it later:

.. code-block:: bash

   sudo rm -rf /opt/awsqueueengine-venv \
       /usr/local/bin/{awsqe-host,awsqe-client,awsqueueengine}

Worker hosts
------------

Worker hosts do **not** need AWSQueueEngine installed. They need SSH
reachability from the queue host, scratch space, and — for S3-backed payloads —
the AWS CLI. See :doc:`worker-hosts`.
