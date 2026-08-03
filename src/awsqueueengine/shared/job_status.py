"""The job status vocabulary, shared by the host lookup and the client ledger.

Five statuses come from the queue host's state files (see
:mod:`awsqueueengine.shared.job_lookup`); three more exist only in the client's
tracked-job ledger, for jobs the host either hasn't been asked about yet or has
no record of.

These are plain ``str`` constants rather than an ``Enum`` on purpose: every one
of these values is JSON on the RPC wire, JSON on disk in the client ledger, and
text in ``run.info``. An Enum would force ``.value`` at each of those boundaries
and, worse, would raise instead of display if a newer queue host ever reports a
status this client doesn't know about.
"""

# Reported by the queue host.
QUEUED = "queued"
RUNNING = "running"
COMPLETED = "completed"
FAILED = "failed"
# Finished, but the monitor had no exit status to prove how. Not "unresolved".
UNKNOWN = "unknown"

# Client-ledger only.
SUBMITTED = "submitted"  # enqueued; never refreshed from the host yet
DELETED = "deleted"      # removed from the queue by this client's `qdel`
MISSING = "missing"      # the host returned no record for this job id

HOST_STATUSES = (QUEUED, RUNNING, COMPLETED, FAILED, UNKNOWN)
CLIENT_STATUSES = (SUBMITTED, DELETED, MISSING)
ALL_STATUSES = HOST_STATUSES + CLIENT_STATUSES

#: Statuses that can never change again, so a refresh can skip them.
TERMINAL_STATUSES = frozenset({COMPLETED, FAILED, UNKNOWN, DELETED})

#: `missing` is deliberately absent here *and* from TERMINAL_STATUSES: it
#: conflates "someone else deleted it", "it aged out of the host's failure
#: history", "wrong queue host", and "we read the host's state file while the
#: monitor was rewriting it" — and that last one is transient. Re-asking costs
#: nothing once the query is batched, so we keep asking without claiming the
#: job is live.
ACTIVE_STATUSES = frozenset({SUBMITTED, QUEUED, RUNNING})

#: Convenience names accepted wherever a status filter is.
STATUS_ALIASES = {
    "active": ACTIVE_STATUSES,
    "done": TERMINAL_STATUSES,
    "all": frozenset(ALL_STATUSES),
}


def is_terminal(status):
    """True when `status` can never change again, so a refresh may skip it."""
    return status in TERMINAL_STATUSES


def expand_status_filter(tokens):
    """Turn CLI ``--status`` tokens into a set of concrete status names.

    Accepts repeated flags and comma-separated values interchangeably, so
    ``--status queued --status running`` and ``--status queued,running`` mean
    the same thing. Aliases in :data:`STATUS_ALIASES` expand to their sets.

    Returns ``None`` for no tokens (meaning "no filter"), and raises
    ``ValueError`` naming the valid statuses for anything unrecognized.
    """
    if not tokens:
        return None
    wanted = set()
    for token in tokens:
        for part in str(token).split(","):
            name = part.strip().lower()
            if not name:
                continue
            if name in STATUS_ALIASES:
                wanted |= STATUS_ALIASES[name]
            elif name in ALL_STATUSES:
                wanted.add(name)
            else:
                raise ValueError(
                    f"unknown status: {part.strip()!r}. "
                    f"Valid: {', '.join(ALL_STATUSES)}. "
                    f"Aliases: {', '.join(sorted(STATUS_ALIASES))}."
                )
    return wanted or None
