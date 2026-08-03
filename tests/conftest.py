"""Safety net: no test may write to the developer's real home directory.

The client's tracked-job ledger lives at ``~/.awsqe/client/jobs.json``, and
``LEDGER_PATH`` is resolved from ``Path.home()`` at import time. Any in-process
test that reaches a client CLI handler (`submit`, `qdel`, `info`, `jobs`) will
therefore mutate the *real* ledger unless it redirects that constant — which is
silent, and pollutes the machine running the suite rather than failing.

``STATE_LOCK_FILE`` is the same story from the host side: every queue mutation
now takes it, so a test that enqueues would otherwise create (and contend on)
``~/.awsqe/host/state.lock`` on the developer's machine — and, worse, contend
with a monitor daemon actually running there.

This autouse fixture redirects all of them for every test. Tests that need to
inspect the ledger still patch ``LEDGER_PATH`` themselves, per this repo's
fixture-per-file convention; this only guarantees the floor.

Caveat: conftest.py is a pytest mechanism. Under ``python -m unittest discover``
it does not run, so the per-file fixtures remain the real protection.
"""
import tempfile
from pathlib import Path

import pytest

from awsqueueengine.client import ledger as ledger_mod
from awsqueueengine.client import logs as logs_mod
from awsqueueengine.shared import state_lock as state_lock_mod


@pytest.fixture(autouse=True)
def _never_touch_the_real_ledger():
    original_ledger = ledger_mod.LEDGER_PATH
    original_logs = logs_mod.LOG_DIR
    original_state_lock = state_lock_mod.STATE_LOCK_FILE
    with tempfile.TemporaryDirectory() as tmp:
        ledger_mod.LEDGER_PATH = Path(tmp) / "jobs.json"
        logs_mod.LOG_DIR = Path(tmp) / "logs"
        state_lock_mod.STATE_LOCK_FILE = Path(tmp) / "state.lock"
        try:
            yield
        finally:
            ledger_mod.LEDGER_PATH = original_ledger
            logs_mod.LOG_DIR = original_logs
            state_lock_mod.STATE_LOCK_FILE = original_state_lock
