"""Tests for awsqueueengine.client.config."""
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from awsqueueengine.client import config as cfg_mod
from awsqueueengine.client.config import (
    DEFAULT_S3_PREFIX,
    ClientConfig,
    _render_toml,
    effective_queue_host,
    effective_s3_bucket,
    effective_s3_prefix,
    get_value,
    load_config,
    normalize_key,
    save_config,
    set_value,
    unset_value,
)


class TempConfigFixture(unittest.TestCase):
    """Each test gets a fresh CONFIG_PATH inside a tempdir."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.config_path = Path(self._tmp.name) / "config.toml"
        self._orig_config_path = cfg_mod.CONFIG_PATH
        cfg_mod.CONFIG_PATH = self.config_path
        # Strip env vars that effective_* helpers consult.
        self._saved_env = {}
        for key in ("AWSQUEUEENGINE_S3_BUCKET", "AWSQUEUEENGINE_S3_PREFIX"):
            self._saved_env[key] = os.environ.pop(key, None)

    def tearDown(self):
        cfg_mod.CONFIG_PATH = self._orig_config_path
        for key, original in self._saved_env.items():
            if original is not None:
                os.environ[key] = original
            else:
                os.environ.pop(key, None)
        self._tmp.cleanup()


class NormalizeKeyTests(unittest.TestCase):
    def test_underscore_and_hyphen_are_equivalent(self):
        self.assertEqual(normalize_key("queue_host"), "queue_host")
        self.assertEqual(normalize_key("queue-host"), "queue_host")
        self.assertEqual(normalize_key("QUEUE-HOST"), "queue_host")
        self.assertEqual(normalize_key("  queue-host  "), "queue_host")

    def test_dotted_keys_work_for_nested_sections(self):
        self.assertEqual(normalize_key("s3.bucket"), "s3.bucket")
        self.assertEqual(normalize_key("S3.BUCKET"), "s3.bucket")

    def test_unknown_key_raises_with_helpful_listing(self):
        with self.assertRaises(ValueError) as ctx:
            normalize_key("not_a_key")
        self.assertIn("not_a_key", str(ctx.exception))
        self.assertIn("queue_host", str(ctx.exception))


class RenderTomlTests(unittest.TestCase):
    def test_default_section_emitted_first_then_s3(self):
        text = _render_toml({"s3": {"bucket": "b"}, "default": {"queue_host": "h"}})
        # default block before s3 block
        self.assertLess(text.index("[default]"), text.index("[s3]"))

    def test_string_values_are_quoted(self):
        text = _render_toml({"default": {"queue_host": "host-with-dashes"}})
        self.assertIn('queue_host = "host-with-dashes"', text)

    def test_escaping_backslashes_and_quotes(self):
        text = _render_toml({"default": {"queue_host": 'tricky "thing" \\ ok'}})
        self.assertIn('queue_host = "tricky \\"thing\\" \\\\ ok"', text)

    def test_empty_input_renders_to_empty_string(self):
        self.assertEqual(_render_toml({}), "")
        self.assertEqual(_render_toml({"default": {}}), "")


class LoadConfigTests(TempConfigFixture):
    def test_missing_file_returns_empty_config(self):
        cfg = load_config()
        self.assertIsNone(cfg.queue_host)
        self.assertIsNone(cfg.s3_bucket)
        self.assertIsNone(cfg.s3_prefix)
        self.assertEqual(cfg.extra, {})

    def test_load_well_formed_file(self):
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        self.config_path.write_text(
            '[default]\nqueue_host = "queue-1"\n\n[s3]\nbucket = "buck"\nprefix = "pfx"\n'
        )
        cfg = load_config()
        self.assertEqual(cfg.queue_host, "queue-1")
        self.assertEqual(cfg.s3_bucket, "buck")
        self.assertEqual(cfg.s3_prefix, "pfx")

    def test_prefix_is_stripped_of_slashes(self):
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        self.config_path.write_text('[s3]\nprefix = "/leading/and/trailing/"\n')
        cfg = load_config()
        self.assertEqual(cfg.s3_prefix, "leading/and/trailing")

    def test_malformed_file_does_not_crash_returns_empty(self):
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        self.config_path.write_text('this is not [valid TOML\n')
        cfg = load_config()  # should not raise
        self.assertIsNone(cfg.queue_host)

    def test_extra_sections_are_preserved_round_trip(self):
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        self.config_path.write_text(
            '[default]\nqueue_host = "q1"\n\n'
            '[experimental]\nfeature_x = "on"\n'
        )
        cfg = load_config()
        self.assertEqual(cfg.queue_host, "q1")
        self.assertIn("experimental", cfg.extra)
        self.assertEqual(cfg.extra["experimental"]["feature_x"], "on")


class SaveAndRoundTripTests(TempConfigFixture):
    def test_save_creates_parent_directory(self):
        nested = self.config_path.parent / "deeper" / "config.toml"
        cfg_mod.CONFIG_PATH = nested
        cfg = ClientConfig(queue_host="q")
        path = save_config(cfg)
        self.assertEqual(path, nested)
        self.assertTrue(nested.exists())

    def test_round_trip_preserves_all_known_fields(self):
        cfg = ClientConfig(queue_host="q1", s3_bucket="buck", s3_prefix="pfx")
        save_config(cfg)
        loaded = load_config()
        self.assertEqual(loaded.queue_host, "q1")
        self.assertEqual(loaded.s3_bucket, "buck")
        self.assertEqual(loaded.s3_prefix, "pfx")

    def test_round_trip_preserves_extra_sections(self):
        cfg = ClientConfig(queue_host="q1", extra={"keep_me": {"k": "v"}})
        save_config(cfg)
        loaded = load_config()
        self.assertEqual(loaded.queue_host, "q1")
        self.assertEqual(loaded.extra.get("keep_me"), {"k": "v"})

    def test_extras_with_special_chars_round_trip_without_breaking_toml(self):
        # Quotes, backslashes, tabs, newlines in an extra value must be
        # escaped on save so the file remains valid TOML.
        tricky = 'has "quote" and \\backslash\\ and\ttab\nand newline'
        cfg = ClientConfig(queue_host="q1", extra={"extras": {"note": tricky}})
        save_config(cfg)
        # The file must still parse as valid TOML.
        loaded = load_config()
        self.assertEqual(loaded.queue_host, "q1")
        self.assertEqual(loaded.extra.get("extras"), {"note": tricky})

    def test_extras_with_non_string_value_round_trip_as_text(self):
        # tomllib hands us lists/dicts/ints for some unknown keys; the
        # emitter must not produce invalid TOML on save. Numbers stay
        # numeric, everything else degrades to a quoted string.
        cfg = ClientConfig(queue_host="q1", extra={"extras": {"weird": ["a", 'b"c']}})
        save_config(cfg)
        # Parses without error — that's the load-bearing assertion.
        loaded = load_config()
        self.assertEqual(loaded.queue_host, "q1")
        self.assertIn("weird", loaded.extra.get("extras", {}))

    def test_save_is_atomic_when_possible(self):
        cfg = ClientConfig(queue_host="q")
        save_config(cfg)
        # No leftover .tmp file.
        self.assertFalse(self.config_path.with_suffix(self.config_path.suffix + ".tmp").exists())


class SetGetUnsetTests(TempConfigFixture):
    def test_set_then_get_each_key(self):
        cfg = ClientConfig()
        set_value(cfg, "queue_host", "qh")
        set_value(cfg, "s3.bucket", "b")
        set_value(cfg, "s3.prefix", "p")
        self.assertEqual(get_value(cfg, "queue_host"), "qh")
        self.assertEqual(get_value(cfg, "s3.bucket"), "b")
        self.assertEqual(get_value(cfg, "s3.prefix"), "p")

    def test_unset_returns_none_for_that_key(self):
        cfg = ClientConfig(queue_host="qh", s3_bucket="b")
        unset_value(cfg, "queue_host")
        self.assertIsNone(get_value(cfg, "queue_host"))
        self.assertEqual(get_value(cfg, "s3.bucket"), "b")

    def test_set_strips_whitespace_and_trailing_slash_on_prefix(self):
        cfg = ClientConfig()
        set_value(cfg, "s3.prefix", "  awsqueueengine/payloads/  ")
        self.assertEqual(get_value(cfg, "s3.prefix"), "awsqueueengine/payloads")

    def test_set_unknown_key_raises(self):
        cfg = ClientConfig()
        with self.assertRaises(ValueError):
            set_value(cfg, "nope", "value")


class EffectiveQueueHostTests(TempConfigFixture):
    def test_cli_flag_wins_over_config(self):
        save_config(ClientConfig(queue_host="from-config"))
        self.assertEqual(effective_queue_host("from-cli"), "from-cli")

    def test_config_used_when_no_cli_flag(self):
        save_config(ClientConfig(queue_host="from-config"))
        self.assertEqual(effective_queue_host(None), "from-config")

    def test_returns_none_when_neither_is_set(self):
        self.assertIsNone(effective_queue_host(None))

    def test_empty_string_cli_treated_as_missing(self):
        save_config(ClientConfig(queue_host="from-config"))
        self.assertEqual(effective_queue_host(""), "from-config")


class EffectiveS3Tests(TempConfigFixture):
    def test_env_wins_over_config_for_bucket(self):
        save_config(ClientConfig(s3_bucket="cfg-bucket"))
        with patch.dict(os.environ, {"AWSQUEUEENGINE_S3_BUCKET": "env-bucket"}):
            self.assertEqual(effective_s3_bucket(), "env-bucket")

    def test_config_used_when_no_env_for_bucket(self):
        save_config(ClientConfig(s3_bucket="cfg-bucket"))
        self.assertEqual(effective_s3_bucket(), "cfg-bucket")

    def test_bucket_returns_none_when_neither_is_set(self):
        self.assertIsNone(effective_s3_bucket())

    def test_env_wins_over_config_for_prefix(self):
        save_config(ClientConfig(s3_prefix="cfg-prefix"))
        with patch.dict(os.environ, {"AWSQUEUEENGINE_S3_PREFIX": "env-prefix"}):
            self.assertEqual(effective_s3_prefix(), "env-prefix")

    def test_prefix_falls_through_to_default(self):
        # No env, no config.
        self.assertEqual(effective_s3_prefix(), DEFAULT_S3_PREFIX)

    def test_env_prefix_is_stripped_of_slashes(self):
        with patch.dict(os.environ, {"AWSQUEUEENGINE_S3_PREFIX": "/foo/bar/"}):
            self.assertEqual(effective_s3_prefix(), "foo/bar")


if __name__ == "__main__":
    unittest.main()
