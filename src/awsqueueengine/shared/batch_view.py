"""Collapsing the host views (`list`, `qstat`) by batch tag.

Lives in ``shared`` rather than in either CLI because both declare these
subcommands, and their *flat* renderers are already near-duplicates of each
other — adding a grouped renderer to each would make that four copies to keep
in step instead of two. The flat renderer stays each CLI's own, passed in as
``render_flat`` so the untagged remainder still looks exactly as it does today.

The client's tracked-job view groups separately
(:func:`awsqueueengine.client.ledger.group_by_array`): those records have a
``submitted_at`` to order and summarize by, which a queue entry does not.
"""
from __future__ import annotations


def distinct_values(items, key):
    """Distinct non-empty values of `key`, in first-seen order."""
    seen = []
    for item in items:
        value = str(item.get(key) or "")
        if value and value not in seen:
            seen.append(value)
    return seen


def shared_or_marker(values):
    """One column, one value: the shared value, ``-`` for none, ``*`` for several.

    A batch usually shares a queue and a priority, and saying so is worth a
    column. When it doesn't, the row must not print one member's value and
    imply the rest agree.
    """
    if not values:
        return "-"
    return values[0] if len(values) == 1 else "*"


def group_items_by_array(items):
    """``(groups, loose)`` over host-view items, keyed on ``array_id``.

    `groups` is ``[(array_id, [item, ...]), ...]`` in first-seen order; the
    queue is priority-ordered and `qstat` is keyed by host, so there is no
    timestamp to sort on here. `loose` is ``[(position, item), ...]`` with the
    **original 1-based position**, because for the queue that number is what
    ``qdel --index`` selects on and must survive being rendered as a subset.
    """
    order, by_array, loose = [], {}, []
    for position, item in enumerate(items, 1):
        array_id = str(item.get("array_id") or "")
        if not array_id:
            loose.append((position, item))
            continue
        if array_id not in by_array:
            by_array[array_id] = []
            order.append(array_id)
        by_array[array_id].append(item)
    return [(name, by_array[name]) for name in order], loose


def render_grouped_queue(jobs, render_flat):
    """`list --group`. `render_flat(jobs, positions)` renders the untagged remainder."""
    groups, loose = group_items_by_array(jobs)
    if not groups:
        render_flat(jobs, None)
        return
    print(f"{'ARRAY':24}  {'JOBS':>6}  {'QUEUE':12}  {'PRI':>6}  CMD", flush=True)
    for array_id, members in groups:
        priorities = sorted({item.get("priority", 0) for item in members})
        print(
            f"{array_id[:24]:24}  {len(members):>6}  "
            f"{shared_or_marker(distinct_values(members, 'queue'))[:12]:12}  "
            f"{(str(priorities[0]) if len(priorities) == 1 else '*'):>6}  "
            f"{str(members[0].get('cmd') or '')}",
            flush=True,
        )
    if loose:
        print("", flush=True)
        render_flat([item for _, item in loose], [position for position, _ in loose])
    print(
        f"{len(jobs)} queued job(s); {len(jobs) - len(loose)} in {len(groups)} batch(es).",
        flush=True,
    )


def render_grouped_running(running, render_flat):
    """`qstat --group`. `running` is ``{host: item}``; `render_flat` takes the same."""
    ordered_hosts = sorted(running)
    groups, _ = group_items_by_array([running[host] for host in ordered_hosts])
    if not groups:
        render_flat(running)
        return
    print(f"{'ARRAY':24}  {'JOBS':>6}  {'QUEUE':12}  {'HOSTS':30}  CMD", flush=True)
    for array_id, members in groups:
        hosts = [host for host in ordered_hosts
                 if str(running[host].get("array_id") or "") == array_id]
        print(
            f"{array_id[:24]:24}  {len(members):>6}  "
            f"{shared_or_marker(distinct_values(members, 'queue'))[:12]:12}  "
            f"{','.join(hosts)[:30]:30}  {str(members[0].get('cmd') or '')}",
            flush=True,
        )
    loose = {host: running[host] for host in ordered_hosts
             if not running[host].get("array_id")}
    if loose:
        print("", flush=True)
        render_flat(loose)
