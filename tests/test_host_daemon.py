"""Tests for host/daemon.py systemd helpers.

Everything is exercised in --dry-run mode or with subprocess/shutil
patches so no real systemctl call ever happens.
"""
import io
import contextlib
import unittest
from unittest.mock import patch

from awsqueueengine.host import daemon


class ResolvePlanTests(unittest.TestCase):
    def test_user_mode_picks_user_paths_and_args(self):
        plan = daemon.resolve_plan(user_mode=True)
        self.assertTrue(plan.user_mode)
        self.assertEqual(plan.systemctl_args, ("systemctl", "--user"))
        self.assertEqual(plan.journalctl_args, ("journalctl", "--user"))
        self.assertEqual(plan.wanted_by, "default.target")
        self.assertIsNone(plan.run_as_user)
        self.assertTrue(str(plan.unit_path).endswith("/.config/systemd/user/awsqe-host.service"))

    def test_system_mode_prefers_sudo_user_for_run_as(self):
        with patch.dict("os.environ", {"SUDO_USER": "ubuntu"}, clear=False):
            plan = daemon.resolve_plan(user_mode=False)
        self.assertFalse(plan.user_mode)
        self.assertEqual(plan.systemctl_args, ("systemctl",))
        self.assertEqual(plan.wanted_by, "multi-user.target")
        self.assertEqual(plan.run_as_user, "ubuntu")
        self.assertEqual(str(plan.unit_path), "/etc/systemd/system/awsqe-host.service")

    def test_system_mode_falls_back_to_USER_when_no_sudo(self):
        env = {"USER": "alice"}
        with patch.dict("os.environ", env, clear=True):
            plan = daemon.resolve_plan(user_mode=False)
        self.assertEqual(plan.run_as_user, "alice")

    def test_system_mode_final_fallback_to_ubuntu(self):
        with patch.dict("os.environ", {}, clear=True):
            plan = daemon.resolve_plan(user_mode=False)
        self.assertEqual(plan.run_as_user, "ubuntu")


class RenderUnitTests(unittest.TestCase):
    def test_user_unit_omits_user_line_and_uses_default_target(self):
        plan = daemon.resolve_plan(user_mode=True)
        unit = daemon.render_unit(plan, exec_start="/path/awsqe-host monitor")
        self.assertNotIn("User=", unit)
        self.assertIn("WantedBy=default.target", unit)
        self.assertIn("ExecStart=/path/awsqe-host monitor", unit)
        self.assertIn("Type=simple", unit)
        self.assertIn("Restart=on-failure", unit)

    def test_system_unit_includes_user_line_and_multiuser_target(self):
        with patch.dict("os.environ", {"SUDO_USER": "ubuntu"}, clear=False):
            plan = daemon.resolve_plan(user_mode=False)
        unit = daemon.render_unit(plan, exec_start="/usr/local/bin/awsqe-host monitor")
        self.assertIn("User=ubuntu", unit)
        self.assertIn("WantedBy=multi-user.target", unit)
        self.assertIn("ExecStart=/usr/local/bin/awsqe-host monitor", unit)

    def test_resolve_exec_start_prefers_installed_console_script(self):
        with patch("awsqueueengine.host.daemon.shutil.which", return_value="/opt/venv/bin/awsqe-host"):
            self.assertEqual(daemon.resolve_exec_start(), "/opt/venv/bin/awsqe-host monitor")

    def test_resolve_exec_start_falls_back_to_python_module(self):
        with patch("awsqueueengine.host.daemon.shutil.which", return_value=None):
            result = daemon.resolve_exec_start()
        self.assertIn("-m awsqueueengine.host.cli monitor", result)


class SystemModeRootCheckTests(unittest.TestCase):
    """Install/uninstall in system mode must return 1 (not sys.exit) when not root,
    and the os.geteuid() lookup must be guarded for non-POSIX platforms."""

    def test_user_mode_never_requires_root(self):
        plan = daemon.resolve_plan(user_mode=True)
        self.assertFalse(daemon._system_mode_needs_root(plan))

    def test_system_mode_requires_root_when_not_root(self):
        plan = daemon.resolve_plan(user_mode=False)
        with patch("awsqueueengine.host.daemon.os.geteuid", return_value=1000):
            self.assertTrue(daemon._system_mode_needs_root(plan))

    def test_system_mode_satisfied_when_running_as_root(self):
        plan = daemon.resolve_plan(user_mode=False)
        with patch("awsqueueengine.host.daemon.os.geteuid", return_value=0):
            self.assertFalse(daemon._system_mode_needs_root(plan))

    def test_platforms_without_geteuid_are_not_blocked(self):
        plan = daemon.resolve_plan(user_mode=False)
        # Simulate Windows: os has no geteuid. Use delattr-and-restore.
        original = daemon.os.geteuid
        del daemon.os.geteuid
        try:
            self.assertFalse(daemon._system_mode_needs_root(plan))
        finally:
            daemon.os.geteuid = original

    def test_install_system_mode_returns_1_when_not_root_instead_of_sysexit(self):
        buf_err = io.StringIO()
        with patch("awsqueueengine.host.daemon.systemctl_available", return_value=True), \
             patch("awsqueueengine.host.daemon.os.geteuid", return_value=1000), \
             contextlib.redirect_stderr(buf_err):
            rc = daemon.install(user_mode=False, force=False, dry_run=True)
        self.assertEqual(rc, 1)
        self.assertIn("requires root", buf_err.getvalue())

    def test_uninstall_system_mode_returns_1_when_not_root_instead_of_sysexit(self):
        buf_err = io.StringIO()
        with patch("awsqueueengine.host.daemon.os.geteuid", return_value=1000), \
             contextlib.redirect_stderr(buf_err):
            rc = daemon.uninstall(user_mode=False, dry_run=True)
        self.assertEqual(rc, 1)
        self.assertIn("requires root", buf_err.getvalue())


class FallbackReturnCodeTests(unittest.TestCase):
    """stop/status fallbacks (no systemctl) propagate cmd_*_monitor's int return."""

    def test_stop_fallback_propagates_int_from_cmd_stop_monitor(self):
        with patch("awsqueueengine.host.daemon.systemctl_available", return_value=False), \
             patch("awsqueueengine.host.cli.cmd_stop_monitor", return_value=7):
            rc = daemon.stop(user_mode=True, dry_run=False)
        self.assertEqual(rc, 7)

    def test_status_fallback_propagates_int_from_cmd_status_monitor(self):
        with patch("awsqueueengine.host.daemon.systemctl_available", return_value=False), \
             patch("awsqueueengine.host.cli.cmd_status_monitor", return_value=42):
            rc = daemon.status(user_mode=True, dry_run=False)
        self.assertEqual(rc, 42)


class DryRunInstallTests(unittest.TestCase):
    def _install(self, **kwargs):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = daemon.install(dry_run=True, **kwargs)
        return rc, buf.getvalue()

    def test_install_user_dry_run_emits_write_and_daemon_reload_and_enable_now(self):
        with patch("awsqueueengine.host.daemon.systemctl_available", return_value=True):
            rc, out = self._install(user_mode=True, force=False)
        self.assertEqual(rc, 0)
        self.assertIn("[dry-run] write unit", out)
        self.assertIn("systemctl --user daemon-reload", out)
        self.assertIn("systemctl --user enable --now awsqe-host", out)
        self.assertIn("WantedBy=default.target", out)

    def test_install_aborts_when_systemctl_not_available(self):
        with patch("awsqueueengine.host.daemon.systemctl_available", return_value=False):
            buf = io.StringIO()
            with contextlib.redirect_stderr(buf):
                rc = daemon.install(user_mode=True, force=False, dry_run=True)
        self.assertEqual(rc, 1)
        self.assertIn("systemctl was not found", buf.getvalue())

    def test_install_refuses_to_overwrite_without_force(self):
        # Pretend the unit file already exists.
        with patch("awsqueueengine.host.daemon.systemctl_available", return_value=True), \
             patch("pathlib.Path.exists", return_value=True):
            buf = io.StringIO()
            with contextlib.redirect_stderr(buf):
                rc = daemon.install(user_mode=True, force=False, dry_run=True)
        self.assertEqual(rc, 1)
        self.assertIn("Pass --force to overwrite", buf.getvalue())

    def test_install_with_force_overwrites(self):
        with patch("awsqueueengine.host.daemon.systemctl_available", return_value=True), \
             patch("pathlib.Path.exists", return_value=True):
            rc, out = self._install(user_mode=True, force=True)
        self.assertEqual(rc, 0)
        self.assertIn("[dry-run] write unit", out)

    def test_dry_run_summary_says_would_install_not_installed(self):
        """Under --dry-run nothing actually happened, so the final summary must
        not falsely claim 'Installed ... and started ...'."""
        with patch("awsqueueengine.host.daemon.systemctl_available", return_value=True):
            rc, out = self._install(user_mode=True, force=False)
        self.assertEqual(rc, 0)
        self.assertIn("[dry-run] would install", out)
        self.assertNotIn("and started", out)

    def test_real_install_summary_says_installed_and_started(self):
        """Sanity check the non-dry-run summary still mentions 'Installed' and
        'started' so we don't accidentally regress the real-mode wording."""
        captured = io.StringIO()
        with patch("awsqueueengine.host.daemon.systemctl_available", return_value=True), \
             patch("awsqueueengine.host.daemon._write_unit"), \
             patch("awsqueueengine.host.daemon._run"), \
             patch("pathlib.Path.exists", return_value=False), \
             contextlib.redirect_stdout(captured):
            rc = daemon.install(user_mode=True, force=False, dry_run=False)
        out = captured.getvalue()
        self.assertEqual(rc, 0)
        self.assertIn("Installed", out)
        self.assertIn("and started", out)
        self.assertNotIn("[dry-run]", out)


class DryRunUninstallTests(unittest.TestCase):
    def test_uninstall_user_dry_run_disables_and_removes_unit(self):
        buf = io.StringIO()
        with patch("awsqueueengine.host.daemon.systemctl_available", return_value=True), \
             patch("pathlib.Path.exists", return_value=True), \
             contextlib.redirect_stdout(buf):
            rc = daemon.uninstall(user_mode=True, dry_run=True)
        out = buf.getvalue()
        self.assertEqual(rc, 0)
        self.assertIn("systemctl --user disable --now awsqe-host", out)
        self.assertIn("[dry-run] rm", out)
        self.assertIn("systemctl --user daemon-reload", out)

    def test_dry_run_summary_says_would_remove_not_removed(self):
        """Under --dry-run the summary must not claim the unit was removed."""
        buf = io.StringIO()
        with patch("awsqueueengine.host.daemon.systemctl_available", return_value=True), \
             patch("pathlib.Path.exists", return_value=True), \
             contextlib.redirect_stdout(buf):
            daemon.uninstall(user_mode=True, dry_run=True)
        out = buf.getvalue()
        self.assertIn("[dry-run] would remove", out)
        # The bare 'Removed <path>.' line (without the [dry-run] prefix) must not appear.
        for line in out.splitlines():
            self.assertNotRegex(line, r"^Removed ")


class StatusAndStartFallbackTests(unittest.TestCase):
    def test_start_with_systemctl_runs_systemctl_start(self):
        captured = {}

        def fake_run(argv, **kwargs):
            captured["argv"] = list(argv)

            class R:
                returncode = 0

            return R()

        with patch("awsqueueengine.host.daemon.systemctl_available", return_value=True), \
             patch("awsqueueengine.host.daemon.subprocess.run", side_effect=fake_run):
            rc = daemon.start(user_mode=True, dry_run=False)
        self.assertEqual(rc, 0)
        self.assertEqual(captured["argv"], ["systemctl", "--user", "start", "awsqe-host"])

    def test_restart_errors_when_systemctl_missing(self):
        with patch("awsqueueengine.host.daemon.systemctl_available", return_value=False):
            buf = io.StringIO()
            with contextlib.redirect_stderr(buf):
                rc = daemon.restart(user_mode=True, dry_run=False)
        self.assertEqual(rc, 1)
        self.assertIn("only meaningful under systemd", buf.getvalue())


class LogsTests(unittest.TestCase):
    def test_logs_builds_journalctl_argv_with_flags(self):
        captured = {}

        def fake_which(name):
            return f"/usr/bin/{name}"

        def fake_run(argv):
            captured["argv"] = list(argv)

            class R:
                returncode = 0

            return R()

        with patch("awsqueueengine.host.daemon.shutil.which", side_effect=fake_which), \
             patch("awsqueueengine.host.daemon.subprocess.run", side_effect=fake_run):
            rc = daemon.logs(user_mode=False, follow=True, lines=50, dry_run=False)
        self.assertEqual(rc, 0)
        self.assertEqual(captured["argv"], ["journalctl", "-u", "awsqe-host", "-f", "-n", "50"])

    def test_logs_user_mode_passes_user_flag(self):
        captured = {}

        def fake_run(argv):
            captured["argv"] = list(argv)

            class R:
                returncode = 0

            return R()

        with patch("awsqueueengine.host.daemon.shutil.which", side_effect=lambda n: f"/usr/bin/{n}"), \
             patch("awsqueueengine.host.daemon.subprocess.run", side_effect=fake_run):
            daemon.logs(user_mode=True, follow=False, lines=None, dry_run=False)
        self.assertEqual(captured["argv"][:3], ["journalctl", "--user", "-u"])

    def test_logs_errors_when_journalctl_missing(self):
        with patch("awsqueueengine.host.daemon.shutil.which", return_value=None):
            buf = io.StringIO()
            with contextlib.redirect_stderr(buf):
                rc = daemon.logs(user_mode=False, follow=False, lines=None, dry_run=False)
        self.assertEqual(rc, 1)
        self.assertIn("journalctl not available", buf.getvalue())

    def test_logs_ctrl_c_during_follow_exits_cleanly_with_130(self):
        # `awsqe-host logs -f` blocks waiting on journalctl; Ctrl-C in the
        # outer shell raises KeyboardInterrupt out of subprocess.run. Without
        # the catch, the user gets a multi-line traceback for what should be
        # a silent exit.
        def fake_run_raises_kbd(argv):
            raise KeyboardInterrupt()

        with patch("awsqueueengine.host.daemon.shutil.which", side_effect=lambda n: f"/usr/bin/{n}"), \
             patch("awsqueueengine.host.daemon.subprocess.run", side_effect=fake_run_raises_kbd):
            rc = daemon.logs(user_mode=False, follow=True, lines=None, dry_run=False)
        self.assertEqual(rc, 130)  # 128 + SIGINT(2)


if __name__ == "__main__":
    unittest.main()
