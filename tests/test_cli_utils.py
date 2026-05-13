"""Tests for shared.cli_utils.join_command_argv."""
import unittest

from awsqueueengine.shared.cli_utils import join_command_argv


class JoinCommandArgvTests(unittest.TestCase):
    def test_empty_argv_returns_empty_string(self):
        self.assertEqual(join_command_argv([]), "")
        self.assertEqual(join_command_argv(None), "")

    def test_strips_leading_double_dash(self):
        # The case the patocontrol smoke test surfaced: argparse REMAINDER
        # keeps the `--` separator. Without the strip, the persisted job cmd
        # would be `'-- echo from-dev-box'` and the worker would try to run it.
        self.assertEqual(
            join_command_argv(["--", "echo", "from-dev-box"]),
            "echo from-dev-box",
        )

    def test_only_double_dash_becomes_empty(self):
        # Caller's empty-check must still catch this.
        self.assertEqual(join_command_argv(["--"]), "")

    def test_does_not_strip_internal_double_dash(self):
        # `--` is only special as the leading token. `bash -- echo` later in the
        # tokens must survive (rare but valid).
        self.assertEqual(
            join_command_argv(["bash", "--", "echo", "hi"]),
            "bash -- echo hi",
        )

    def test_passes_through_a_normal_command(self):
        self.assertEqual(
            join_command_argv(["python", "train.py", "--epochs", "10"]),
            "python train.py --epochs 10",
        )


if __name__ == "__main__":
    unittest.main()
