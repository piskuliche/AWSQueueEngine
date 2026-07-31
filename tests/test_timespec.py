"""Tests for the --since/--until time argument parser."""
import unittest
from datetime import datetime, timedelta

from awsqueueengine.shared.run_info import format_epoch
from awsqueueengine.shared.timespec import parse_time_spec


class AbsoluteFormTests(unittest.TestCase):
    def test_date_only_is_local_midnight(self):
        self.assertEqual(
            parse_time_spec("2026-07-30"),
            datetime(2026, 7, 30, 0, 0, 0).timestamp(),
        )

    def test_date_and_time_forms(self):
        expected = datetime(2026, 7, 30, 14, 15, 0).timestamp()
        self.assertEqual(parse_time_spec("2026-07-30 14:15"), expected)
        self.assertEqual(parse_time_spec("2026-07-30T14:15"), expected)
        self.assertEqual(parse_time_spec("2026-07-30 14:15:00"), expected)
        self.assertEqual(parse_time_spec("2026-07-30T14:15:00"), expected)

    def test_surrounding_whitespace_is_ignored(self):
        self.assertEqual(parse_time_spec("  2026-07-30  "), parse_time_spec("2026-07-30"))

    def test_round_trips_with_format_epoch(self):
        """What a timestamp column prints must parse back to the same second."""
        epoch = datetime(2026, 7, 30, 14, 15, 30).timestamp()
        self.assertEqual(parse_time_spec(format_epoch(epoch)), epoch)


class EndOfDayTests(unittest.TestCase):
    def test_date_only_upper_bound_covers_the_whole_day(self):
        """`--until 2026-07-30` means "through the 30th", not "up to its 00:00"."""
        self.assertEqual(
            parse_time_spec("2026-07-30", end_of_day=True),
            datetime(2026, 7, 31, 0, 0, 0).timestamp(),
        )

    def test_end_of_day_does_not_shift_an_explicit_time(self):
        expected = datetime(2026, 7, 30, 14, 15, 0).timestamp()
        self.assertEqual(parse_time_spec("2026-07-30 14:15", end_of_day=True), expected)

    def test_end_of_day_does_not_shift_a_relative_span(self):
        now = datetime(2026, 7, 30, 14, 0, 0)
        self.assertEqual(
            parse_time_spec("1h", now=now, end_of_day=True),
            parse_time_spec("1h", now=now),
        )


class RelativeFormTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 7, 30, 12, 0, 0)

    def _ago(self, **kwargs):
        return (self.now - timedelta(**kwargs)).timestamp()

    def test_each_unit(self):
        for text, delta in (
            ("45s", {"seconds": 45}),
            ("30m", {"minutes": 30}),
            ("24h", {"hours": 24}),
            ("7d", {"days": 7}),
            ("2w", {"weeks": 2}),
        ):
            with self.subTest(text=text):
                self.assertEqual(parse_time_spec(text, now=self.now), self._ago(**delta))

    def test_unit_is_case_insensitive_and_tolerates_a_space(self):
        self.assertEqual(parse_time_spec("7D", now=self.now), self._ago(days=7))
        self.assertEqual(parse_time_spec("7 d", now=self.now), self._ago(days=7))

    def test_zero_is_now(self):
        self.assertEqual(parse_time_spec("0d", now=self.now), self.now.timestamp())


class RejectionTests(unittest.TestCase):
    def test_bare_number_is_rejected_as_ambiguous_with_a_year(self):
        with self.assertRaises(ValueError):
            parse_time_spec("2026")

    def test_timezone_suffixes_are_rejected_rather_than_parsed_as_local(self):
        for text in ("2026-07-30T14:15:00Z", "2026-07-30T14:15:00+01:00"):
            with self.subTest(text=text), self.assertRaises(ValueError):
                parse_time_spec(text)

    def test_empty_and_non_string(self):
        for value in ("", "   ", None, 12345):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_time_spec(value)

    def test_nonsense_and_impossible_dates(self):
        for text in ("yesterday", "2026-13-01", "2026-07-32", "7y"):
            with self.subTest(text=text), self.assertRaises(ValueError):
                parse_time_spec(text)

    def test_error_message_names_the_accepted_forms(self):
        with self.assertRaises(ValueError) as ctx:
            parse_time_spec("yesterday")
        message = str(ctx.exception)
        self.assertIn("yesterday", message)
        self.assertIn("YYYY-MM-DD", message)
        self.assertIn("7d", message)


if __name__ == "__main__":
    unittest.main()
