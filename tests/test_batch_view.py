"""`list --group` / `qstat --group`: collapsing the host views by batch tag.

The load-bearing case is queue positions. The number `list` prints in the left
column is what `qdel --index` selects on, so rendering the untagged remainder as
a *subset* must not renumber it — otherwise `--group` quietly turns `--index`
into a way to delete the wrong job.
"""
import io
import unittest
from contextlib import redirect_stdout

from awsqueueengine.shared.batch_view import (
    distinct_values,
    group_items_by_array,
    render_grouped_queue,
    render_grouped_running,
    shared_or_marker,
)


def _job(cmd, array_id=None, queue="default", priority=0):
    return {"cmd": cmd, "array_id": array_id, "queue": queue, "priority": priority}


class GroupItemsTests(unittest.TestCase):
    def test_untagged_items_are_all_loose_with_their_positions(self):
        groups, loose = group_items_by_array([_job("a"), _job("b")])
        self.assertEqual(groups, [])
        self.assertEqual([position for position, _ in loose], [1, 2])

    def test_loose_items_keep_their_original_queue_positions(self):
        items = [_job("a", "batch"), _job("loose"), _job("c", "batch"), _job("d")]
        _, loose = group_items_by_array(items)
        # Positions 2 and 4, not 1 and 2 — these are qdel --index targets.
        self.assertEqual([position for position, _ in loose], [2, 4])

    def test_groups_are_in_first_seen_order(self):
        items = [_job("a", "second"), _job("b", "first"), _job("c", "second")]
        groups, _ = group_items_by_array(items)
        self.assertEqual([name for name, _ in groups], ["second", "first"])
        self.assertEqual(len(groups[0][1]), 2)


class MarkerTests(unittest.TestCase):
    def test_no_values_is_a_dash(self):
        self.assertEqual(shared_or_marker([]), "-")

    def test_one_shared_value_is_named(self):
        self.assertEqual(shared_or_marker(["gpu"]), "gpu")

    def test_several_values_are_marked_not_sampled(self):
        self.assertEqual(shared_or_marker(["gpu", "default"]), "*")

    def test_distinct_values_skips_blanks_and_keeps_order(self):
        items = [{"q": "b"}, {"q": ""}, {"q": "a"}, {"q": "b"}, {}]
        self.assertEqual(distinct_values(items, "q"), ["b", "a"])


class RenderGroupedQueueTests(unittest.TestCase):
    def _render(self, jobs):
        calls = []

        def render_flat(items, positions=None):
            calls.append((items, positions))
            print("FLAT", flush=True)

        out = io.StringIO()
        with redirect_stdout(out):
            render_grouped_queue(jobs, render_flat)
        return out.getvalue(), calls

    def test_an_untagged_queue_delegates_entirely_to_the_flat_renderer(self):
        out, calls = self._render([_job("a"), _job("b")])
        self.assertEqual(out, "FLAT\n")
        self.assertEqual(len(calls), 1)
        self.assertIsNone(calls[0][1])

    def test_a_batch_becomes_one_row(self):
        out, _ = self._render([_job("x", "batch", priority=-100)] * 3)
        self.assertIn("ARRAY", out)
        rows = [line for line in out.splitlines() if line.startswith("batch")]
        self.assertEqual(len(rows), 1)
        self.assertIn("3", rows[0].split())
        self.assertIn("3 queued job(s); 3 in 1 batch(es).", out)

    def test_the_untagged_remainder_is_rendered_at_its_real_positions(self):
        jobs = [_job("a", "batch"), _job("loose1"), _job("c", "batch"), _job("loose2")]
        _, calls = self._render(jobs)
        self.assertEqual(calls[0][1], [2, 4])

    def test_a_shared_priority_is_named_and_a_mixed_one_is_marked(self):
        out, _ = self._render([_job("x", "b", priority=-100), _job("y", "b", priority=-100)])
        self.assertIn("-100", out)
        out, _ = self._render([_job("x", "b", priority=-100), _job("y", "b", priority=5)])
        self.assertNotIn("-100", out)
        self.assertIn("*", out)

    def test_a_batch_spanning_two_queues_is_marked(self):
        out, _ = self._render([_job("x", "b", queue="gpu"), _job("y", "b", queue="default")])
        self.assertNotIn("gpu", out)
        self.assertIn("*", out)


class RenderGroupedRunningTests(unittest.TestCase):
    def _render(self, running):
        calls = []

        def render_flat(items):
            calls.append(items)
            print("FLAT", flush=True)

        out = io.StringIO()
        with redirect_stdout(out):
            render_grouped_running(running, render_flat)
        return out.getvalue(), calls

    def test_untagged_running_jobs_delegate_entirely(self):
        out, calls = self._render({"eci1": _job("a"), "eci2": _job("b")})
        self.assertEqual(out, "FLAT\n")
        self.assertEqual(len(calls), 1)

    def test_a_batch_row_lists_the_hosts_it_is_running_on(self):
        out, _ = self._render({
            "eci7": _job("x", "batch"),
            "eci5": _job("x", "batch"),
            "eci9": _job("other"),
        })
        row = next(line for line in out.splitlines() if line.startswith("batch"))
        self.assertIn("eci5,eci7", row)
        self.assertIn("2", row.split())

    def test_the_untagged_remainder_still_renders(self):
        _, calls = self._render({"eci5": _job("x", "batch"), "eci9": _job("loose")})
        self.assertEqual(list(calls[0]), ["eci9"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
