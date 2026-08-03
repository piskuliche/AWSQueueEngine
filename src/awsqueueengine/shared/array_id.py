"""The batch tag (``array_id``), shared by the client ledger and the queue host.

A batch here is a *tag*, not a Slurm-style array: the jobs share a command and
each carries its own payload, so there is no index to vary. That measurement is
what closed the true-array proposal, and it is why this module is 40 lines
rather than a scheduler change.

:func:`validate_array_id` **rejects** a name it would have to rewrite, rather
than sanitizing it silently the way
:func:`~awsqueueengine.shared.queue_config.normalize_queue_name` does. The
difference matters because a queue name is chosen once by an administrator
while an array name is a label the user types *back* in, at
``jobs --array`` and ``qdel --array``. A submit that silently became
``ffpopt_IDC`` would leave ``qdel --array 'ffpopt IDC'`` matching nothing, with
nothing on screen to explain why.

Both sides normalize through here so the two can never disagree about what a
given tag is: the client validates at submit, so only already-clean names reach
the wire, and :func:`normalize_array_id` then only has to strip.
"""
from __future__ import annotations

#: Bounds the ARRAY column in `jobs` and `list` output.
ARRAY_ID_MAX_LENGTH = 64

#: Punctuation allowed inside a name (never at either end).
ALLOWED_PUNCTUATION = "._-"


def _sanitize(text):
    cleaned = "".join(ch if ch.isalnum() or ch in ALLOWED_PUNCTUATION else "_" for ch in text)
    return cleaned.strip(ALLOWED_PUNCTUATION)


def normalize_array_id(value):
    """The stored form of a batch tag, or ``None``.

    Lenient on purpose: this runs over data that is already persisted, where
    refusing a value would mean dropping it from the record. User input goes
    through :func:`validate_array_id` first. Mirrors
    :func:`~awsqueueengine.shared.queue._normalize_job_id`.
    """
    if not isinstance(value, str):
        return None
    return value.strip() or None


def validate_array_id(value):
    """Return a batch tag fit to store, or raise ``ValueError`` saying why not.

    ``None`` and an unset flag pass straight through as ``None``; an *empty*
    string does not, because typing ``--array ''`` is a mistake rather than a
    way to opt out.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"array name must be text, got {type(value).__name__}")
    clean = value.strip()
    if not clean:
        raise ValueError("array name cannot be empty")
    if len(clean) > ARRAY_ID_MAX_LENGTH:
        raise ValueError(
            f"array name is {len(clean)} characters; the limit is {ARRAY_ID_MAX_LENGTH}"
        )
    sanitized = _sanitize(clean)
    if sanitized != clean:
        suggestion = sanitized or "batch"
        raise ValueError(
            f"invalid array name {clean!r}: use letters, digits, '.', '_' and '-' only, "
            f"and neither end may be punctuation. Try {suggestion!r}."
        )
    return clean
