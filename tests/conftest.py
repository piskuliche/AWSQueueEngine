"""Safety net: no test may write to the developer's real home directory.

The client's tracked-job ledger lives at ``~/.awsqe/client/jobs.json``, and
``LEDGER_PATH`` is resolved from ``Path.home()`` at import time. Any in-process
test that reaches a client CLI handler (`submit`, `qdel`, `info`, `jobs`) will
therefore mutate the *real* ledger unless it redirects that constant — which is
silent, and pollutes the machine running the suite rather than failing.

This autouse fixture redirects it for every test. Tests that need to inspect
the ledger still patch ``LEDGER_PATH`` themselves, per this repo's
fixture-per-file convention; this only guarantees the floor.

Caveat: conftest.py is a pytest mechanism. Under ``python -m unittest discover``
it does not run, so the per-file fixtures remain the real protection.
"""
import tempfile
from pathlib import Path

import pytest

from awsqueueengine.client import ledger as ledger_mod
from awsqueueengine.client import logs as logs_mod


@pytest.fixture(autouse=True)
def _never_touch_the_real_ledger():
    original_ledger = ledger_mod.LEDGER_PATH
    original_logs = logs_mod.LOG_DIR
    with tempfile.TemporaryDirectory() as tmp:
        ledger_mod.LEDGER_PATH = Path(tmp) / "jobs.json"
        logs_mod.LOG_DIR = Path(tmp) / "logs"
        try:
            yield
        finally:
            ledger_mod.LEDGER_PATH = original_ledger
            logs_mod.LOG_DIR = original_logs
