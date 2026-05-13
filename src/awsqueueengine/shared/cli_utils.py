"""Tiny utilities shared by the client and host CLIs.

``join_command_argv`` exists because every submit subcommand uses
``nargs=argparse.REMAINDER`` for the trailing command tokens. argparse's
REMAINDER does NOT consume the ``--`` separator the user typed before the
command, so without this helper a ``submit --queue-host h -- echo hi``
would persist the cmd as ``'-- echo hi'`` and the worker would try to run
``-- echo hi`` (which silently succeeds in bash but is clearly wrong).
"""
from __future__ import annotations


def join_command_argv(argv: list[str] | None) -> str:
    """Join the REMAINDER argv into a single command string, stripping a
    leading ``--`` separator if argparse left it in place."""
    if not argv:
        return ""
    tokens = list(argv)
    if tokens and tokens[0] == "--":
        tokens = tokens[1:]
    return " ".join(tokens).strip()
