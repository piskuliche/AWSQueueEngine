Email alerts
============

The monitor can send email through the Mailtrap API. Configure the credentials
via the systemd unit drop-in — see :doc:`queue-host`.

Settings
--------

.. list-table::
   :header-rows: 1
   :widths: 52 12 36

   * - Variable
     - Default
     - Meaning
   * - ``AWSQUEUEENGINE_MAILTRAP_TOKEN``
     - —
     - **Required** for any email to send.
   * - ``AWSQUEUEENGINE_MAILTRAP_SENDER_EMAIL``
     - —
     - **Required**; the ``From`` address.
   * - ``AWSQUEUEENGINE_MAILTRAP_SENDER_NAME``
     - ``AWSQueueEngine``
     - Display name in the ``From`` header.
   * - ``AWSQUEUEENGINE_MAILTRAP_CATEGORY``
     - ``Queue Monitor``
     - Tags emails on the Mailtrap side.
   * - ``AWSQUEUEENGINE_ALERT_TO``
     - —
     - Comma-separated recipients.
   * - ``AWSQUEUEENGINE_ALERT_DAILY_EMAIL_LIMIT``
     - ``150``
     - Cap on total outgoing emails per day.
   * - ``AWSQUEUEENGINE_JOB_FAIL_ALERT_COOLDOWN_SECONDS``
     - ``900``
     - Rate limit between job-failure emails.

When it sends
-------------

1. When a queued job fails to start on a host.
2. Once when queue depth drops below 10 — on the transition into the low-queue
   state, not repeatedly.
3. Once when queue depth reaches 0, likewise on transition.
4. Once per calendar day, when the monitor detects a new date, with a status
   summary.
5. When a host is placed in cooldown after a storage or transport failure.

Both rate limits apply on top of these: the daily cap bounds everything, and
the failure cooldown keeps a flapping host from flooding your inbox.

Testing the credentials
-----------------------

.. code-block:: bash

   awsqe-host --test-email-connection

.. important::

   This reads credentials from the **calling shell's** environment, not from
   the systemd unit — so a pass here does not prove the daemon is configured.
   To check the unit's view, use ``sudo systemctl show awsqe-host -p Environment``
   and let the daemon's next alert-eligible event fire.
