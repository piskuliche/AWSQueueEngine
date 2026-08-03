"""Unit tests for the qdel selector resolver in ``shared.queue``.

These exercise ``resolve_queue_selection`` against a plain list, with no
state files involved — the persistence half lives in ``remove_queue_positions``
and is covered by the RPC and CLI tests.
"""
import argparse
import unittest

from awsqueueengine.shared.cli_utils import (
    add_qdel_arguments,
    qdel_selectors,
    validate_qdel_selectors,
)
from awsqueueengine.shared.queue import (
    QueueSelectionError,
    resolve_queue_selection,
)


def _queue(*job_ids):
    return [{"cmd": f"echo {jid}", "job_id": jid} for jid in job_ids]


class ResolveByJobIdTests(unittest.TestCase):
    def test_exact_ids_resolve_to_positions(self):
        q = _queue("aaa111", "bbb222", "ccc333")
        selection = resolve_queue_selection(q, job_ids=["ccc333", "aaa111"])
        self.assertEqual([pos for pos, _ in selection], [0, 2])

    def test_unique_prefix_resolves(self):
        q = _queue("aaa111", "bbb222")
        selection = resolve_queue_selection(q, job_ids=["bbb"])
        self.assertEqual([pos for pos, _ in selection], [1])

    def test_matching_is_case_insensitive(self):
        q = _queue("20260730-141530-a1b2c3")
        selection = resolve_queue_selection(q, job_ids=["20260730-141530-A1B2C3"])
        self.assertEqual([pos for pos, _ in selection], [0])

    def test_ambiguous_prefix_is_a_conflict(self):
        q = _queue("aaa111", "aaa222")
        with self.assertRaises(QueueSelectionError) as cm:
            resolve_queue_selection(q, job_ids=["aaa"])
        self.assertEqual(cm.exception.code, "conflict")
        self.assertIn("aaa111", cm.exception.message)
        self.assertIn("aaa222", cm.exception.message)

    def test_exact_match_wins_over_prefix_of_another_id(self):
        """A full id that also prefixes a longer one is never ambiguous."""
        q = _queue("aaa", "aaa111")
        selection = resolve_queue_selection(q, job_ids=["aaa"])
        self.assertEqual([pos for pos, _ in selection], [0])

    def test_unknown_id_is_not_found_and_carries_the_token(self):
        q = _queue("aaa111")
        with self.assertRaises(QueueSelectionError) as cm:
            resolve_queue_selection(q, job_ids=["zzz999"])
        self.assertEqual(cm.exception.code, "not_found")
        self.assertEqual(cm.exception.tokens, ["zzz999"])

    def test_all_unknown_ids_are_reported_together(self):
        q = _queue("aaa111")
        with self.assertRaises(QueueSelectionError) as cm:
            resolve_queue_selection(q, job_ids=["zzz999", "yyy888"])
        self.assertEqual(cm.exception.tokens, ["zzz999", "yyy888"])

    def test_duplicate_tokens_collapse_to_one_position(self):
        q = _queue("aaa111", "bbb222")
        selection = resolve_queue_selection(q, job_ids=["aaa111", "aaa1", "aaa111"])
        self.assertEqual([pos for pos, _ in selection], [0])

    def test_entries_without_a_job_id_never_match(self):
        q = [{"cmd": "legacy"}, {"cmd": "echo a", "job_id": "aaa111"}]
        with self.assertRaises(QueueSelectionError) as cm:
            resolve_queue_selection(q, job_ids=[""])
        self.assertEqual(cm.exception.code, "invalid_params")


class ResolveByIndexTests(unittest.TestCase):
    def test_indices_keep_their_original_meaning(self):
        q = _queue("aaa111", "bbb222", "ccc333")
        selection = resolve_queue_selection(q, indices=[3, 1])
        self.assertEqual([pos for pos, _ in selection], [0, 2])

    def test_duplicate_indices_collapse(self):
        q = _queue("aaa111", "bbb222")
        selection = resolve_queue_selection(q, indices=[2, 2])
        self.assertEqual([pos for pos, _ in selection], [1])

    def test_out_of_range_index_is_a_conflict(self):
        q = _queue("aaa111")
        with self.assertRaises(QueueSelectionError) as cm:
            resolve_queue_selection(q, indices=[5])
        self.assertEqual(cm.exception.code, "conflict")
        self.assertIn("queue size 1", cm.exception.message)

    def test_zero_and_negative_indices_are_rejected(self):
        q = _queue("aaa111")
        with self.assertRaises(QueueSelectionError) as cm:
            resolve_queue_selection(q, indices=[0])
        self.assertEqual(cm.exception.code, "conflict")


class ResolveByQueueTests(unittest.TestCase):
    def _mixed_queue(self):
        return [
            {"cmd": "a", "job_id": "aaa", "queue": "fast"},
            {"cmd": "b", "job_id": "bbb"},
            {"cmd": "c", "job_id": "ccc", "queue": "fast"},
        ]

    def test_queue_name_selects_every_matching_entry(self):
        selection = resolve_queue_selection(self._mixed_queue(), queue="fast")
        self.assertEqual([pos for pos, _ in selection], [0, 2])

    def test_queue_name_is_normalized(self):
        # normalize_queue_name strips and sanitizes but preserves case, so the
        # selector matches queue names exactly as the rest of the system does.
        selection = resolve_queue_selection(self._mixed_queue(), queue="  fast ")
        self.assertEqual([pos for pos, _ in selection], [0, 2])

    def test_empty_queue_name_match_is_not_found(self):
        with self.assertRaises(QueueSelectionError) as cm:
            resolve_queue_selection(self._mixed_queue(), queue="slow")
        self.assertEqual(cm.exception.code, "not_found")


class ResolveByArrayTests(unittest.TestCase):
    def _batched_queue(self):
        return [
            {"cmd": "a", "job_id": "aaa", "array_id": "ffpopt-IDC"},
            {"cmd": "b", "job_id": "bbb"},
            {"cmd": "c", "job_id": "ccc", "array_id": "ffpopt-IDC"},
            {"cmd": "d", "job_id": "ddd", "array_id": "other"},
        ]

    def test_array_name_selects_every_member(self):
        selection = resolve_queue_selection(self._batched_queue(), array_id="ffpopt-IDC")
        self.assertEqual([pos for pos, _ in selection], [0, 2])

    def test_selector_token_names_the_flag(self):
        selection = resolve_queue_selection(self._batched_queue(), array_id="ffpopt-IDC")
        self.assertEqual({token for _, token in selection}, {"--array ffpopt-IDC"})

    def test_matching_is_case_insensitive_and_trimmed(self):
        selection = resolve_queue_selection(self._batched_queue(), array_id="  FFPOPT-idc ")
        self.assertEqual([pos for pos, _ in selection], [0, 2])

    def test_a_prefix_does_not_match(self):
        """Unlike a job id, where a prefix is a typing convenience. Here it would
        silently widen a destructive operation from one batch to every batch
        whose name starts the same way."""
        with self.assertRaises(QueueSelectionError) as cm:
            resolve_queue_selection(self._batched_queue(), array_id="ffpopt")
        self.assertEqual(cm.exception.code, "not_found")

    def test_unknown_array_is_not_found_and_removes_nothing(self):
        q = self._batched_queue()
        with self.assertRaises(QueueSelectionError) as cm:
            resolve_queue_selection(q, array_id="nope")
        self.assertEqual(cm.exception.code, "not_found")
        self.assertIn("nope", cm.exception.message)
        self.assertEqual(len(q), 4)

    def test_untagged_jobs_are_never_selected(self):
        selection = resolve_queue_selection(self._batched_queue(), array_id="other")
        self.assertEqual([pos for pos, _ in selection], [3])


class SelectorCombinationTests(unittest.TestCase):
    def test_no_selector_is_invalid_params(self):
        with self.assertRaises(QueueSelectionError) as cm:
            resolve_queue_selection(_queue("aaa111"))
        self.assertEqual(cm.exception.code, "invalid_params")

    def test_mixing_selectors_is_invalid_params(self):
        with self.assertRaises(QueueSelectionError) as cm:
            resolve_queue_selection(_queue("aaa111"), job_ids=["aaa111"], indices=[1])
        self.assertEqual(cm.exception.code, "invalid_params")
        self.assertIn("cannot combine", cm.exception.message)

    def test_mixing_array_with_job_ids_is_invalid_params(self):
        with self.assertRaises(QueueSelectionError) as cm:
            resolve_queue_selection(
                _queue("aaa111"), job_ids=["aaa111"], array_id="ffpopt-IDC",
            )
        self.assertEqual(cm.exception.code, "invalid_params")
        self.assertIn("cannot combine", cm.exception.message)

    def test_no_selector_message_names_the_array_option(self):
        with self.assertRaises(QueueSelectionError) as cm:
            resolve_queue_selection(_queue("aaa111"))
        self.assertIn("array", cm.exception.message)

    def test_empty_queue_is_not_found(self):
        with self.assertRaises(QueueSelectionError) as cm:
            resolve_queue_selection([], job_ids=["aaa111"])
        self.assertEqual(cm.exception.code, "not_found")
        self.assertIn("queue is empty", cm.exception.message)


class QdelArgumentTests(unittest.TestCase):
    """The one arg surface shared by the client, host, and legacy parsers."""

    def _parse(self, argv):
        parser = argparse.ArgumentParser()
        add_qdel_arguments(parser)
        return parser.parse_args(argv)

    def test_positionals_are_job_ids_not_ints(self):
        args = self._parse(["aaa111", "bbb222"])
        self.assertEqual(qdel_selectors(args), (["aaa111", "bbb222"], [], None, None))
        self.assertIsNone(validate_qdel_selectors(args))

    def test_index_flag_parses_positions(self):
        args = self._parse(["--index", "1", "3"])
        self.assertEqual(qdel_selectors(args), ([], [1, 3], None, None))
        self.assertIsNone(validate_qdel_selectors(args))

    def test_queue_flag_parses(self):
        args = self._parse(["--queue", "fast"])
        self.assertEqual(qdel_selectors(args), ([], [], "fast", None))
        self.assertIsNone(validate_qdel_selectors(args))

    def test_bare_integer_points_at_the_index_flag(self):
        args = self._parse(["3"])
        message = validate_qdel_selectors(args)
        self.assertIn("looks like a queue index", message)
        self.assertIn("--index 3", message)

    def test_date_prefix_is_treated_as_a_job_id_not_an_index(self):
        """Job ids open with an 8-digit date, so `20260730` is a real prefix."""
        args = self._parse(["20260730"])
        self.assertIsNone(validate_qdel_selectors(args))
        self.assertEqual(qdel_selectors(args), (["20260730"], [], None, None))

    def test_no_selector_is_rejected(self):
        self.assertIn("Provide one or more", validate_qdel_selectors(self._parse([])))

    def test_combined_selectors_are_rejected(self):
        args = self._parse(["aaa111", "--queue", "fast"])
        self.assertIn("Cannot combine", validate_qdel_selectors(args))

    def test_array_flag_parses(self):
        args = self._parse(["--array", "ffpopt-IDC"])
        self.assertEqual(qdel_selectors(args), ([], [], None, "ffpopt-IDC"))
        self.assertIsNone(validate_qdel_selectors(args))

    def test_array_combined_with_another_selector_is_rejected(self):
        args = self._parse(["--array", "ffpopt-IDC", "--queue", "fast"])
        self.assertIn("Cannot combine", validate_qdel_selectors(args))

    def test_no_selector_message_offers_array(self):
        self.assertIn("--array", validate_qdel_selectors(self._parse([])))

    def test_selectors_are_addressable_by_name(self):
        """The NamedTuple is what keeps the next selector from churning every
        unpack site again; it still compares equal to a plain tuple."""
        args = self._parse(["--array", "ffpopt-IDC"])
        self.assertEqual(qdel_selectors(args).array_id, "ffpopt-IDC")
        self.assertEqual(qdel_selectors(args).job_ids, [])


if __name__ == "__main__":
    unittest.main()
