"""Tiny utilities shared by the client and host CLIs.

``add_qdel_arguments`` lives here because ``qdel`` is declared in three
parsers (client, host, legacy shim) whose selector surfaces must not drift,
and the client is not allowed to import from the host package.

``join_command_argv`` exists because every submit subcommand uses
``nargs=argparse.REMAINDER`` for the trailing command tokens. argparse's
REMAINDER does NOT consume the ``--`` separator the user typed before the
command, so without this helper a ``submit --queue-host h -- echo hi``
would persist the cmd as ``'-- echo hi'`` and the worker would try to run
``-- echo hi`` (which silently succeeds in bash but is clearly wrong).
"""
from __future__ import annotations

from typing import NamedTuple


def join_command_argv(argv: list[str] | None) -> str:
    """Join the REMAINDER argv into a single command string, stripping a
    leading ``--`` separator if argparse left it in place."""
    if not argv:
        return ""
    tokens = list(argv)
    if tokens and tokens[0] == "--":
        tokens = tokens[1:]
    return " ".join(tokens).strip()


QDEL_HELP = "Delete queued job(s) by job id"


def add_qdel_arguments(parser) -> None:
    """Declare the qdel selectors on ``parser``."""
    parser.add_argument(
        "job_ids",
        nargs="*",
        metavar="JOB_ID",
        help="Job id(s) as shown by `list` ([job=...]); a unique prefix also works",
    )
    parser.add_argument(
        "--index",
        dest="indices",
        nargs="+",
        type=int,
        default=[],
        metavar="N",
        help=(
            "Delete by 1-based position in `list` instead. Positions shift as jobs "
            "launch, requeue, or get preempted, so prefer job ids."
        ),
    )
    parser.add_argument(
        "--queue",
        dest="queue",
        default=None,
        metavar="NAME",
        help="Delete every queued job in NAME",
    )
    parser.add_argument(
        "--array",
        dest="array_id",
        default=None,
        metavar="NAME",
        help=(
            "Delete every queued job tagged into batch NAME (see "
            "`awsqe-client submit --array`). Matched exactly — no prefixes, "
            "since this selector is destructive."
        ),
    )


class QdelSelectors(NamedTuple):
    """The cleaned selector set off a parsed namespace.

    A NamedTuple rather than a plain tuple so the next selector added here does
    not churn every unpack site again; it still compares equal to a 4-tuple, so
    existing tests and callers that unpack positionally keep working.
    """

    job_ids: list[str]
    indices: list[int]
    queue: "str | None"
    array_id: "str | None"


def qdel_selectors(args) -> QdelSelectors:
    """The cleaned selectors off a parsed namespace."""
    return QdelSelectors(
        job_ids=[str(t).strip() for t in (getattr(args, "job_ids", None) or []) if str(t).strip()],
        indices=list(getattr(args, "indices", None) or []),
        queue=(getattr(args, "queue", None) or "").strip() or None,
        array_id=(getattr(args, "array_id", None) or "").strip() or None,
    )


def validate_qdel_selectors(args) -> str | None:
    """User-facing error text for an unusable selector combination, else ``None``."""
    job_ids, indices, queue, array_id = qdel_selectors(args)

    # A short bare integer is almost certainly the pre-job-id syntax; say so
    # rather than reporting it as an unknown job id. Job ids open with an
    # 8-digit date, so anything that long stays a job id prefix and resolves
    # (or reports ambiguity) against the real queue instead.
    numeric = [token for token in job_ids if token.isdigit() and len(token) < 8]
    if numeric:
        sample = numeric[0]
        return (
            f"{sample!r} looks like a queue index, not a job id. Job ids look like "
            f"20260730-141530-a1b2c3 (see `list`). Use `--index {sample}` to delete by position."
        )

    kinds = [
        name
        for name, present in (
            ("job ids", job_ids),
            ("--index", indices),
            ("--queue", queue),
            ("--array", array_id),
        )
        if present
    ]
    if not kinds:
        return "Provide one or more job ids, --index N, --queue NAME, or --array NAME."
    if len(kinds) > 1:
        return f"Cannot combine {' and '.join(kinds)}; pick one selector."
    return None
