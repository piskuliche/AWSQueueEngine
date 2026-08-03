"""Parse the ``--since`` / ``--until`` style time arguments into epoch seconds.

Accepts an absolute timestamp (``2026-07-30``, ``2026-07-30 14:15``,
``2026-07-30T14:15:00``) or a relative span meaning "that long ago"
(``30m``, ``24h``, ``7d``, ``2w``).

**Timezone:** absolute forms are parsed as *local* time, matching
:func:`awsqueueengine.shared.run_info.format_epoch`, which renders epochs with
``datetime.fromtimestamp``. So whatever a timestamp column prints can be pasted
straight back in as a filter and mean the same instant. Timezone suffixes
(``Z``, ``+01:00``) are rejected rather than silently mis-parsed as local.
"""
import re
from datetime import datetime, timedelta

_ABSOLUTE_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M",
)

_DATE_ONLY_FORMAT = "%Y-%m-%d"

# A bare number is rejected: "2026" is far likelier to be a mistyped year than
# 2026 of some unit.
_RELATIVE_RE = re.compile(r"^(\d+)\s*([smhdw])$", re.IGNORECASE)

_RELATIVE_UNITS = {
    "s": "seconds",
    "m": "minutes",
    "h": "hours",
    "d": "days",
    "w": "weeks",
}

ACCEPTED_FORMS = (
    "YYYY-MM-DD, 'YYYY-MM-DD HH:MM[:SS]', YYYY-MM-DDTHH:MM[:SS], "
    "or a relative span like 30m / 24h / 7d / 2w"
)


def parse_time_spec(text, *, now=None, end_of_day=False):
    """Return epoch seconds for `text`.

    `now` (a ``datetime``) anchors relative spans; it defaults to the current
    local time and exists so tests don't have to freeze the clock.

    `end_of_day` changes what a *date-only* argument means: normally
    ``2026-07-30`` is that day's 00:00, but for an upper bound the user means
    "through the 30th", so it resolves to the following midnight. Forms that
    name a time of day are unaffected — those are always exact.
    """
    if not isinstance(text, str) or not text.strip():
        raise ValueError(f"empty time value; expected {ACCEPTED_FORMS}")
    raw = text.strip()

    relative = _RELATIVE_RE.match(raw)
    if relative:
        amount = int(relative.group(1))
        unit = _RELATIVE_UNITS[relative.group(2).lower()]
        anchor = now or datetime.now()
        return (anchor - timedelta(**{unit: amount})).timestamp()

    for fmt in _ABSOLUTE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).timestamp()
        except ValueError:
            continue

    try:
        midnight = datetime.strptime(raw, _DATE_ONLY_FORMAT)
    except ValueError:
        raise ValueError(f"unrecognized time value: {text!r}; expected {ACCEPTED_FORMS}") from None
    if end_of_day:
        # Add a calendar day rather than 86400 seconds, so the bound lands on
        # the real next local midnight even across a DST transition.
        return (midnight + timedelta(days=1)).timestamp()
    return midnight.timestamp()
