"""The batch tag vocabulary shared by the client ledger and the queue host.

The point of interest is that `validate_array_id` *rejects* rather than
sanitizing: a tag is typed back in at `jobs --array` and `qdel --array`, so a
silent rewrite at submit would leave the later lookup matching nothing.
"""
import unittest

from awsqueueengine.shared.array_id import (
    ARRAY_ID_MAX_LENGTH,
    normalize_array_id,
    validate_array_id,
)


class NormalizeTests(unittest.TestCase):
    def test_strips_surrounding_whitespace(self):
        self.assertEqual(normalize_array_id("  ffpopt-IDC \n"), "ffpopt-IDC")

    def test_empty_and_whitespace_only_are_none(self):
        self.assertIsNone(normalize_array_id(""))
        self.assertIsNone(normalize_array_id("   "))

    def test_non_string_is_none(self):
        for value in (None, 7, [], {}, True):
            self.assertIsNone(normalize_array_id(value))

    def test_case_is_preserved(self):
        self.assertEqual(normalize_array_id("FFPopt"), "FFPopt")

    def test_already_stored_oddity_is_kept_rather_than_dropped(self):
        # Lenient on purpose: this runs over persisted data, where refusing a
        # value would mean losing the batch tag off an existing record.
        self.assertEqual(normalize_array_id("legacy name"), "legacy name")


class ValidateTests(unittest.TestCase):
    def test_accepts_a_plain_name(self):
        self.assertEqual(validate_array_id("ffpopt-IDC"), "ffpopt-IDC")

    def test_accepts_the_full_allowed_charset(self):
        self.assertEqual(validate_array_id("a.b_c-1Z"), "a.b_c-1Z")

    def test_unset_stays_unset(self):
        self.assertIsNone(validate_array_id(None))

    def test_surrounding_whitespace_is_trimmed_not_rejected(self):
        self.assertEqual(validate_array_id("  ffpopt  "), "ffpopt")

    def test_empty_string_is_rejected_rather_than_treated_as_unset(self):
        # `--array ''` is a typo, not a way to opt out.
        with self.assertRaises(ValueError) as ctx:
            validate_array_id("")
        self.assertIn("cannot be empty", str(ctx.exception))

    def test_a_space_is_rejected_and_a_usable_name_suggested(self):
        with self.assertRaises(ValueError) as ctx:
            validate_array_id("ffpopt IDC")
        message = str(ctx.exception)
        self.assertIn("ffpopt IDC", message)
        self.assertIn("ffpopt_IDC", message)

    def test_a_slash_is_rejected(self):
        with self.assertRaises(ValueError):
            validate_array_id("ffpopt/IDC")

    def test_leading_or_trailing_punctuation_is_rejected(self):
        for value in ("-ffpopt", "ffpopt-", "_ffpopt", "ffpopt."):
            with self.assertRaises(ValueError, msg=value):
                validate_array_id(value)

    def test_length_is_capped(self):
        self.assertEqual(len(validate_array_id("a" * ARRAY_ID_MAX_LENGTH)),
                         ARRAY_ID_MAX_LENGTH)
        with self.assertRaises(ValueError) as ctx:
            validate_array_id("a" * (ARRAY_ID_MAX_LENGTH + 1))
        self.assertIn(str(ARRAY_ID_MAX_LENGTH), str(ctx.exception))

    def test_non_string_is_rejected(self):
        with self.assertRaises(ValueError):
            validate_array_id(7)

    def test_a_validated_name_survives_normalize_unchanged(self):
        # The two sides agree only if this holds: the client validates at
        # submit, the host normalizes on read, and `qdel --array` matches
        # exactly. A name that changed here would match nothing.
        for value in ("ffpopt-IDC", "a.b_c-1Z", "x"):
            self.assertEqual(normalize_array_id(validate_array_id(value)), value)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
