"""Client-side tracked-job list: `awsqe-client jobs`.

Covers the parser surface, the refresh path against the queue host (including
the fallback to an older host and what happens when one host is unreachable),
and the rendering.
"""
import io
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from awsqueueengine.client import cli as client_cli
from awsqueueengine.client import ledger as ledger_mod
from awsqueueengine.client import logs as logs_mod
from awsqueueengine.shared.protocol import RpcError, RpcTransportError


class _JobsArgs:
    """The attribute surface `cmd_jobs` reads off argparse."""

    def __init__(self, **kwargs):
        self.status = None
        self.queue = None
        self.array = None
        self.expand = False
        self.since = None
        self.until = None
        self.limit = 50
        self.no_refresh = False
        self.fetch_logs = False
        self.log = None
        self.cat = None
        self.forget = None
        self.forget_before = None
        self.queue_host = None
        self.__dict__.update(kwargs)


class _LedgerFixture(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.path = Path(self.tmpdir.name) / "jobs.json"
        self._original = ledger_mod.LEDGER_PATH
        ledger_mod.LEDGER_PATH = self.path

    def tearDown(self):
        ledger_mod.LEDGER_PATH = self._original
        self.tmpdir.cleanup()

    def _submit(self, job_id, *, queue_host="qh1", submitted_at=1000.0, **kwargs):
        return ledger_mod.record_submission(
            job_id=job_id, queue_host=queue_host, submitted_at=submitted_at, **kwargs
        )

    def _run(self, args=None, rpc=None):
        """Run cmd_jobs, returning (stdout, stderr)."""
        out, err = io.StringIO(), io.StringIO()
        patcher = patch("awsqueueengine.client.cli.rpc_call",
                        side_effect=rpc if rpc else RuntimeError("unexpected RPC"))
        with patcher, patch("awsqueueengine.client.cli.effective_queue_host", return_value=None):
            with redirect_stdout(out), redirect_stderr(err):
                client_cli.cmd_jobs(args or _JobsArgs(no_refresh=True))
        return out.getvalue(), err.getvalue()


class ParserTests(unittest.TestCase):
    def test_defaults(self):
        args = client_cli.build_parser().parse_args(["jobs"])
        self.assertEqual(args.cmd, "jobs")
        self.assertEqual(args.limit, 50)
        self.assertIsNone(args.status)
        self.assertFalse(args.no_refresh)

    def test_repeated_and_comma_separated_status_both_land(self):
        args = client_cli.build_parser().parse_args(
            ["jobs", "--status", "queued", "--status", "running,failed"]
        )
        self.assertEqual(args.status, ["queued", "running,failed"])

    def test_full_flag_surface(self):
        args = client_cli.build_parser().parse_args([
            "jobs", "--since", "7d", "--until", "2026-07-30", "-n", "5",
            "--no-refresh", "--queue-host", "qh",
        ])
        self.assertEqual(args.since, "7d")
        self.assertEqual(args.until, "2026-07-30")
        self.assertEqual(args.limit, 5)
        self.assertTrue(args.no_refresh)
        self.assertEqual(args.queue_host, "qh")

    def test_grouping_is_on_by_default(self):
        args = client_cli.build_parser().parse_args(["jobs"])
        self.assertFalse(args.expand)
        self.assertIsNone(args.array)

    def test_expand_and_no_group_are_the_same_flag(self):
        for flag in ("--expand", "--no-group"):
            args = client_cli.build_parser().parse_args(["jobs", flag])
            self.assertTrue(args.expand, flag)

    def test_array_is_repeatable(self):
        args = client_cli.build_parser().parse_args(
            ["jobs", "--array", "a", "--array", "b,c"]
        )
        self.assertEqual(args.array, ["a", "b,c"])

    def test_submit_takes_a_single_array_tag(self):
        args = client_cli.build_parser().parse_args(
            ["submit", "--array", "ffpopt-IDC", "python", "t.py"]
        )
        self.assertEqual(args.array, "ffpopt-IDC")

    def test_forget_flags(self):
        args = client_cli.build_parser().parse_args(
            ["jobs", "--forget", "A", "--forget", "B", "--forget-before", "2026-01-01"]
        )
        self.assertEqual(args.forget, ["A", "B"])
        self.assertEqual(args.forget_before, "2026-01-01")

    def test_jobs_does_not_require_a_queue_host(self):
        """`jobs` must work with no config and no reachable host."""
        with patch("awsqueueengine.client.cli.cmd_jobs") as handler, \
             patch("awsqueueengine.client.cli._resolve_queue_host") as resolve:
            client_cli.dispatch(client_cli.build_parser().parse_args(["jobs"]))
        handler.assert_called_once()
        resolve.assert_not_called()


class RefreshTests(_LedgerFixture):
    def test_groups_by_recorded_queue_host_and_skips_terminal_records(self):
        self._submit("A", queue_host="qh1")
        self._submit("B", queue_host="qh1")
        self._submit("C", queue_host="qh2")
        self._submit("DONE", queue_host="qh1")
        ledger_mod.apply_state("DONE", {"status": "completed"})

        calls = []

        def fake_rpc(host, method, params, **kwargs):
            calls.append((host, method, params))
            return {"states": {j: {"status": "running", "host": "eci1"}
                               for j in params["job_ids"]}, "skipped": []}

        self._run(_JobsArgs(), rpc=fake_rpc)

        self.assertEqual([c[0] for c in calls], ["qh1", "qh2"])   # sorted, one call each
        self.assertEqual(calls[0][1], "job_info_batch")
        self.assertEqual(sorted(calls[0][2]["job_ids"]), ["A", "B"])
        self.assertEqual(calls[1][2]["job_ids"], ["C"])
        # The terminal record was never asked about.
        asked = {j for _, _, p in calls for j in p["job_ids"]}
        self.assertNotIn("DONE", asked)

    def test_no_refresh_makes_zero_rpc_calls(self):
        self._submit("A")
        out, _ = self._run(_JobsArgs(no_refresh=True))   # rpc=None raises if called
        self.assertIn("A", out)

    def test_refreshed_state_is_written_back(self):
        self._submit("A")
        self._run(_JobsArgs(), rpc=lambda h, m, p, **k: {
            "states": {"A": {"status": "running", "host": "eci7"}}, "skipped": [],
        })
        record = ledger_mod.load_ledger()[0]
        self.assertEqual(record["status"], "running")
        self.assertEqual(record["host"], "eci7")

    def test_null_state_becomes_missing(self):
        self._submit("A")
        self._submit("B")
        self._run(_JobsArgs(), rpc=lambda h, m, p, **k: {
            "states": {"A": {"status": "running"}, "B": None}, "skipped": [],
        })
        by_id = {r["job_id"]: r for r in ledger_mod.load_ledger()}
        self.assertEqual(by_id["B"]["status"], "missing")
        self.assertIn("missing_since", by_id["B"])

    def test_an_all_null_batch_is_treated_as_a_bad_read_not_mass_deletion(self):
        """The host writes its state files non-atomically; an empty read is suspect."""
        self._submit("A")
        self._submit("B")
        _, err = self._run(_JobsArgs(), rpc=lambda h, m, p, **k: {
            "states": {"A": None, "B": None}, "skipped": [],
        })
        self.assertIn("bad read", err)
        for record in ledger_mod.load_ledger():
            self.assertEqual(record["status"], "submitted")

    def test_settled_ledger_is_not_rewritten(self):
        self._submit("A")
        ledger_mod.apply_state("A", {"status": "running", "host": "eci7"})
        before = self.path.stat().st_mtime_ns
        self._run(_JobsArgs(), rpc=lambda h, m, p, **k: {
            "states": {"A": {"status": "running", "host": "eci7"}}, "skipped": [],
        })
        self.assertEqual(self.path.stat().st_mtime_ns, before)

    def test_skipped_ids_are_re_requested(self):
        """A host with a smaller batch cap answers with the remainder in `skipped`."""
        for job_id in ("A", "B", "C"):
            self._submit(job_id)
        rounds = []

        def fake_rpc(host, method, params, **kwargs):
            asked = params["job_ids"]
            rounds.append(list(asked))
            head, tail = asked[:2], asked[2:]
            return {"states": {j: {"status": "running"} for j in head}, "skipped": tail}

        self._run(_JobsArgs(), rpc=fake_rpc)
        self.assertEqual(len(rounds), 2)
        self.assertEqual(len(rounds[1]), 1)
        self.assertEqual({r["status"] for r in ledger_mod.load_ledger()}, {"running"})

    def test_queue_host_flag_overrides_each_records_host(self):
        self._submit("A", queue_host="qh1")
        self._submit("B", queue_host="qh2")
        calls = []
        self._run(
            _JobsArgs(queue_host="override"),
            rpc=lambda h, m, p, **k: (calls.append(h), {"states": {j: {"status": "running"}
                                                                  for j in p["job_ids"]},
                                                        "skipped": []})[1],
        )
        self.assertEqual(calls, ["override"])


class FallbackAndDegradationTests(_LedgerFixture):
    def test_unknown_method_falls_back_to_per_job_lookups(self):
        self._submit("A")
        self._submit("B")
        calls = []

        def fake_rpc(host, method, params, **kwargs):
            calls.append(method)
            if method == "job_info_batch":
                raise RpcError("unknown_method", "no such method: job_info_batch")
            return {"state": {"status": "running", "host": "eci1"}}

        out, err = self._run(_JobsArgs(), rpc=fake_rpc)

        self.assertEqual(calls, ["job_info_batch", "job_info", "job_info"])
        self.assertIn("older awsqe-host", err)
        self.assertIn("--no-refresh", err)
        self.assertEqual({r["status"] for r in ledger_mod.load_ledger()}, {"running"})

    def test_other_rpc_errors_do_not_trigger_the_fallback(self):
        self._submit("A")
        calls = []

        def fake_rpc(host, method, params, **kwargs):
            calls.append(method)
            raise RpcError("internal", "boom")

        out, err = self._run(_JobsArgs(), rpc=fake_rpc)
        self.assertEqual(calls, ["job_info_batch"])
        self.assertIn("refused the request", err)
        self.assertIn("A", out)   # still listed, with its last-known status

    def test_one_unreachable_host_does_not_stop_the_others(self):
        self._submit("A", queue_host="qh1")
        self._submit("B", queue_host="qh2")

        def fake_rpc(host, method, params, **kwargs):
            if host == "qh1":
                raise RpcTransportError(255, "ssh: connect to host qh1 port 22: No route to host")
            return {"states": {"B": {"status": "running", "host": "eci2"}}, "skipped": []}

        out, err = self._run(_JobsArgs(), rpc=fake_rpc)

        self.assertIn("could not reach qh1", err)
        self.assertIn("1 job(s) not refreshed", err)
        by_id = {r["job_id"]: r for r in ledger_mod.load_ledger()}
        self.assertEqual(by_id["B"]["status"], "running")   # the reachable host still refreshed
        self.assertEqual(by_id["A"]["status"], "submitted")  # last-known preserved
        self.assertIn("A", out)

    def test_records_with_no_queue_host_anywhere_are_skipped_quietly(self):
        self._submit("A", queue_host="")
        out, err = self._run(_JobsArgs())   # rpc=None would raise if called
        self.assertIn("A", out)
        self.assertEqual(err, "")


class RenderTests(_LedgerFixture):
    def test_empty_ledger_and_empty_filter_say_different_things(self):
        out, _ = self._run(_JobsArgs(no_refresh=True))
        self.assertIn("(no tracked jobs)", out)

        self._submit("A")
        out, _ = self._run(_JobsArgs(no_refresh=True, status=["failed"]))
        self.assertIn("(no tracked jobs match the filter)", out)

    def test_newest_first_with_a_footer_when_filtered(self):
        self._submit("OLD", submitted_at=100.0)
        self._submit("NEW", submitted_at=300.0)
        out, _ = self._run(_JobsArgs(no_refresh=True, limit=1))
        rows = [line for line in out.splitlines() if "NEW" in line or "OLD" in line]
        self.assertEqual(len(rows), 1)
        self.assertIn("NEW", rows[0])
        self.assertIn("1 of 2 tracked job(s).", out)

    def test_header_columns(self):
        self._submit("A")
        out, _ = self._run(_JobsArgs(no_refresh=True))
        self.assertIn("SUBMITTED", out)
        for column in ("JOB", "STATUS", "HOST", "QUEUE", "DUR", "CMD"):
            self.assertIn(column, out)

    def test_failed_row_gets_a_continuation_line(self):
        self._submit("A")
        ledger_mod.apply_state("A", {
            "status": "failed", "failure_reason": "out_of_memory", "exit_code": "137",
        })
        out, _ = self._run(_JobsArgs(no_refresh=True))
        self.assertIn("-> out_of_memory exit=137", out)
        self.assertIn("awsqe-client failed --job-id A --log", out)

    def test_queued_row_shows_its_position_in_the_dur_column(self):
        self._submit("A")
        ledger_mod.apply_state("A", {"status": "queued", "queue_position": 3})
        out, _ = self._run(_JobsArgs(no_refresh=True))
        self.assertIn("q#3", out)


class GroupedRenderTests(_LedgerFixture):
    """The batch view. Grouping is on by default, so the load-bearing assertion
    here is that a ledger with no tagged jobs still renders byte-for-byte as it
    did before batches existed."""

    def _batch(self, name, count, *, base=1000.0, queue=None, **kwargs):
        for i in range(count):
            self._submit(f"{name}-{i}", submitted_at=base + i, array_id=name,
                         queue=queue, **kwargs)

    def _array_row(self, out, name):
        """The batch row for `name`, excluding the header line."""
        rows = [line for line in out.splitlines()
                if name in line.split() and not line.startswith("SUBMITTED")]
        self.assertEqual(len(rows), 1, out)
        return rows[0]

    def test_an_untagged_ledger_renders_exactly_as_the_flat_listing(self):
        self._submit("A", submitted_at=100.0)
        self._submit("B", submitted_at=200.0)
        grouped, _ = self._run(_JobsArgs(no_refresh=True))
        expanded, _ = self._run(_JobsArgs(no_refresh=True, expand=True))
        self.assertEqual(grouped, expanded)

    def test_an_untagged_filtered_listing_keeps_the_original_footer_rule(self):
        self._submit("A", submitted_at=100.0)
        self._submit("B", submitted_at=200.0)
        out, _ = self._run(_JobsArgs(no_refresh=True, limit=1))
        self.assertIn("1 of 2 tracked job(s).", out)

    def test_a_batch_collapses_to_one_row(self):
        self._batch("ffpopt-IDC", 5)
        out, _ = self._run(_JobsArgs(no_refresh=True))
        self.assertIn("ARRAY", out)
        self.assertIn("ffpopt-IDC", out)
        # Not one row per job.
        self.assertEqual(sum(1 for line in out.splitlines() if "ffpopt-IDC" in line), 1)
        for i in range(5):
            self.assertNotIn(f"ffpopt-IDC-{i} ", out)

    def test_the_row_summarizes_the_statuses(self):
        self._batch("b", 3)
        ledger_mod.apply_state("b-0", {"status": "completed"})
        ledger_mod.apply_state("b-1", {"status": "failed"})
        out, _ = self._run(_JobsArgs(no_refresh=True))
        self.assertIn("1 submitted", out)
        self.assertIn("1 completed", out)
        self.assertIn("1 failed", out)

    def test_expand_lists_every_job_individually(self):
        self._batch("b", 3)
        out, _ = self._run(_JobsArgs(no_refresh=True, expand=True))
        self.assertNotIn("ARRAY", out)
        for i in range(3):
            self.assertIn(f"b-{i}", out)

    def test_array_filter_drills_into_one_batch_and_implies_expand(self):
        self._batch("keep", 2, base=1000.0)
        self._batch("drop", 2, base=2000.0)
        out, _ = self._run(_JobsArgs(no_refresh=True, array=["keep"]))
        self.assertNotIn("ARRAY", out)
        self.assertIn("keep-0", out)
        self.assertNotIn("drop-0", out)

    def test_array_filter_is_case_insensitive_and_comma_separated(self):
        self._batch("one", 1, base=1000.0)
        self._batch("two", 1, base=2000.0)
        out, _ = self._run(_JobsArgs(no_refresh=True, array=["ONE,two"]))
        self.assertIn("one-0", out)
        self.assertIn("two-0", out)

    def test_batches_and_loose_jobs_appear_in_the_same_listing(self):
        self._batch("b", 3, base=1000.0)
        self._submit("loose", submitted_at=5000.0)
        out, _ = self._run(_JobsArgs(no_refresh=True))
        self.assertIn("ARRAY", out)
        self.assertIn("loose", out)
        self.assertIn("4 of 4 tracked job(s); 3 in 1 batch(es).", out)

    def test_limit_counts_rows_not_records(self):
        """`-n 1` over a 5-job batch must not render `JOBS 1` — the one number
        in that row a reader has no way to second-guess."""
        self._batch("b", 5)
        out, _ = self._run(_JobsArgs(no_refresh=True, limit=1))
        row = self._array_row(out, "b")
        self.assertIn("5", row.split())
        self.assertIn("5 of 5 tracked job(s); 5 in 1 batch(es).", out)

    def test_limit_takes_the_newest_rows_across_both_kinds(self):
        self._batch("old", 2, base=100.0)
        self._submit("new", submitted_at=9000.0)
        out, _ = self._run(_JobsArgs(no_refresh=True, limit=1))
        self.assertIn("new", out)
        self.assertNotIn("old", out)

    def test_status_filter_narrows_the_batch_row(self):
        self._batch("b", 3)
        ledger_mod.apply_state("b-0", {"status": "failed"})
        out, _ = self._run(_JobsArgs(no_refresh=True, status=["failed"]))
        self.assertIn("1 failed", out)
        self.assertIn("1 of 3 tracked job(s); 1 in 1 batch(es).", out)

    def test_a_filtered_row_never_shows_the_submitted_size_fraction(self):
        """`1/142` is true of an evicted batch and false of a filtered one, and
        the row cannot say which."""
        self._batch("b", 3, array_size=142)
        ledger_mod.apply_state("b-0", {"status": "failed"})
        filtered, _ = self._run(_JobsArgs(no_refresh=True, status=["failed"]))
        self.assertNotIn("/142", filtered)
        unfiltered, _ = self._run(_JobsArgs(no_refresh=True))
        self.assertIn("3/142", unfiltered)

    def test_a_batch_spanning_two_queues_is_marked_rather_than_picking_one(self):
        self._submit("m-0", submitted_at=100.0, array_id="m", queue="gpu")
        self._submit("m-1", submitted_at=200.0, array_id="m", queue="default")
        out, _ = self._run(_JobsArgs(no_refresh=True))
        row = self._array_row(out, "m")
        self.assertIn("*", row)
        self.assertNotIn("gpu", row)

    def test_a_batch_on_one_queue_names_it(self):
        self._batch("b", 2, queue="gpu")
        out, _ = self._run(_JobsArgs(no_refresh=True))
        self.assertIn("gpu", out)

    def test_empty_ledger_still_says_no_tracked_jobs(self):
        out, _ = self._run(_JobsArgs(no_refresh=True))
        self.assertIn("(no tracked jobs)", out)


class FilterErrorTests(_LedgerFixture):
    def _expect_exit(self, args, code):
        with self.assertRaises(SystemExit) as ctx:
            self._run(args)
        self.assertEqual(ctx.exception.code, code)

    def test_unknown_status_exits_2_and_lists_the_valid_ones(self):
        self._submit("A")
        out = io.StringIO()
        with self.assertRaises(SystemExit) as ctx, redirect_stdout(out):
            client_cli.cmd_jobs(_JobsArgs(no_refresh=True, status=["runing"]))
        self.assertEqual(ctx.exception.code, 2)
        self.assertIn("unknown status", out.getvalue())
        self.assertIn("running", out.getvalue())

    def test_aliases_expand(self):
        self._submit("ACTIVE")
        self._submit("DONE")
        ledger_mod.apply_state("DONE", {"status": "completed"})
        out, _ = self._run(_JobsArgs(no_refresh=True, status=["active"]))
        self.assertIn("ACTIVE", out)
        self.assertNotIn("DONE", out)

    def test_queue_filter(self):
        self._submit("GPU-JOB", queue="gpu")
        self._submit("CPU-JOB", queue="default")
        out, _ = self._run(_JobsArgs(no_refresh=True, queue=["gpu"]))
        self.assertIn("GPU-JOB", out)
        self.assertNotIn("CPU-JOB", out)

    def test_queue_filter_is_repeatable_and_comma_separated(self):
        self._submit("A", queue="gpu")
        self._submit("B", queue="bigmem")
        self._submit("C", queue="default")
        out, _ = self._run(_JobsArgs(no_refresh=True, queue=["gpu,bigmem"]))
        self.assertIn("A", out)
        self.assertIn("B", out)
        self.assertNotIn("  C ", out)

    def test_queue_filter_normalizes_like_the_host_does(self):
        """`my queue` is stored as `my_queue`, so it must filter as `my_queue` too."""
        self._submit("A", queue="my_queue")
        out, _ = self._run(_JobsArgs(no_refresh=True, queue=["my queue"]))
        self.assertIn("A", out)

    def test_queue_filter_is_case_insensitive(self):
        self._submit("A", queue="zeke-queue")
        out, _ = self._run(_JobsArgs(no_refresh=True, queue=["Zeke-Queue"]))
        self.assertIn("A", out)

    def test_parser_wires_queue(self):
        args = client_cli.build_parser().parse_args(
            ["jobs", "--queue", "gpu", "--queue", "bigmem,fast"]
        )
        self.assertEqual(args.queue, ["gpu", "bigmem,fast"])

    def test_forget_cannot_be_combined_with_a_queue_filter(self):
        self._submit("A", queue="gpu")
        out = io.StringIO()
        with self.assertRaises(SystemExit) as ctx, redirect_stdout(out):
            client_cli.cmd_jobs(_JobsArgs(forget=["A"], queue=["gpu"]))
        self.assertEqual(ctx.exception.code, 2)
        self.assertIn("--queue", out.getvalue())
        self.assertEqual(len(ledger_mod.load_ledger()), 1)

    def test_bad_time_value_exits_2(self):
        self._submit("A")
        self._expect_exit(_JobsArgs(no_refresh=True, since="yesterday"), 2)

    def test_until_bare_date_covers_the_whole_day(self):
        import datetime
        noon = datetime.datetime(2026, 7, 30, 12, 0, 0).timestamp()
        self._submit("SAMEDAY", submitted_at=noon)
        out, _ = self._run(_JobsArgs(no_refresh=True, until="2026-07-30"))
        self.assertIn("SAMEDAY", out)


class ForgetTests(_LedgerFixture):
    def test_forget_removes_and_reports(self):
        self._submit("20260730-141530-a1b2c3")
        out, _ = self._run(_JobsArgs(forget=["20260730-141530-a1b2c3"]))
        self.assertIn("Forgot 1 tracked job(s).", out)
        self.assertEqual(ledger_mod.load_ledger(), [])

    def test_forget_accepts_a_unique_prefix(self):
        self._submit("20260730-141530-a1b2c3")
        self._run(_JobsArgs(forget=["20260730-1415"]))
        self.assertEqual(ledger_mod.load_ledger(), [])

    def test_forget_is_all_or_nothing_on_an_unknown_token(self):
        self._submit("A")
        with self.assertRaises(SystemExit) as ctx:
            self._run(_JobsArgs(forget=["A", "NOPE"]))
        self.assertEqual(ctx.exception.code, 1)
        self.assertEqual(len(ledger_mod.load_ledger()), 1)

    def test_ambiguous_prefix_removes_nothing(self):
        self._submit("20260730-141530-a1b2c3")
        self._submit("20260730-141530-d4e5f6")
        out = io.StringIO()
        with self.assertRaises(SystemExit) as ctx, redirect_stdout(out):
            client_cli.cmd_jobs(_JobsArgs(forget=["20260730-1415"]))
        self.assertEqual(ctx.exception.code, 1)
        self.assertIn("matches 2 tracked jobs", out.getvalue())
        self.assertEqual(len(ledger_mod.load_ledger()), 2)

    def test_forgetting_a_live_job_warns_that_it_is_not_a_cancel(self):
        self._submit("A")
        ledger_mod.apply_state("A", {"status": "running", "host": "eci1"})
        out, _ = self._run(_JobsArgs(forget=["A"]))
        self.assertIn("does not cancel it", out)
        self.assertIn("qdel", out)

    def test_forget_before(self):
        self._submit("OLD", submitted_at=100.0)
        self._submit("NEW", submitted_at=99999999999.0)
        out, _ = self._run(_JobsArgs(forget_before="2026-01-01"))
        self.assertIn("Forgot 1 tracked job(s).", out)
        self.assertEqual([r["job_id"] for r in ledger_mod.load_ledger()], ["NEW"])

    def test_forget_cannot_be_combined_with_a_filter(self):
        self._submit("A")
        out = io.StringIO()
        with self.assertRaises(SystemExit) as ctx, redirect_stdout(out):
            client_cli.cmd_jobs(_JobsArgs(forget=["A"], status=["running"]))
        self.assertEqual(ctx.exception.code, 2)
        self.assertIn("not a filter", out.getvalue())
        self.assertEqual(len(ledger_mod.load_ledger()), 1)

    def test_forget_makes_no_rpc_calls(self):
        self._submit("A")
        self._run(_JobsArgs(forget=["A"]))   # rpc=None raises if called


class FetchLogsTests(_LedgerFixture):
    """`jobs --fetch-logs` / `--log`, with scp stubbed out."""

    def setUp(self):
        super().setUp()
        self.logroot = Path(self.tmpdir.name) / "logs"
        self._orig_logdir = logs_mod.LOG_DIR
        logs_mod.LOG_DIR = self.logroot

    def tearDown(self):
        logs_mod.LOG_DIR = self._orig_logdir
        super().tearDown()

    def _finished(self, job_id, *, host="eci7", status="completed"):
        self._submit(job_id)
        ledger_mod.apply_state(job_id, {"status": status, "host": host,
                                        "finished_at": "2026-07-31 18:10:14"})

    def _run_with_scp(self, args, *, returncode=0, stderr="", body="log body\n"):
        calls = []

        def fake_fetch(job_id, host, *, dest=None, root=None, timeout=None, runner=None):
            calls.append((job_id, host))
            if returncode == 0:
                path = logs_mod.local_log_path(job_id, root=root)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(body)
                return {"ok": True, "path": str(path), "reason": "", "detail": ""}
            lowered = stderr.lower()
            reason = "missing" if "no such file" in lowered else "error"
            return {"ok": False, "path": None, "reason": reason, "detail": stderr}

        out, err = io.StringIO(), io.StringIO()
        with patch.object(logs_mod, "fetch_log", side_effect=fake_fetch), \
             patch.object(logs_mod, "scp_available", return_value=True), \
             patch("awsqueueengine.client.cli.rpc_call",
                   side_effect=RuntimeError("unexpected RPC")), \
             patch("awsqueueengine.client.cli.effective_queue_host", return_value=None):
            with redirect_stdout(out), redirect_stderr(err):
                client_cli.cmd_jobs(args)
        return out.getvalue(), err.getvalue(), calls

    def test_parser_wires_the_flags(self):
        args = client_cli.build_parser().parse_args(["jobs", "--fetch-logs"])
        self.assertTrue(args.fetch_logs)
        args = client_cli.build_parser().parse_args(["jobs", "--log", "JOB-A"])
        self.assertEqual(args.log, "JOB-A")

    def test_fetch_logs_pulls_and_records_the_path(self):
        self._finished("J1")
        out, _, calls = self._run_with_scp(_JobsArgs(no_refresh=True, fetch_logs=True))
        self.assertEqual(calls, [("J1", "eci7")])
        record = ledger_mod.load_ledger()[0]
        self.assertTrue(record["log_path"].endswith("J1.log"))
        self.assertIn("log: ", out)

    def test_second_run_does_not_refetch_a_cached_log(self):
        self._finished("J1")
        self._run_with_scp(_JobsArgs(no_refresh=True, fetch_logs=True))
        _, _, calls = self._run_with_scp(_JobsArgs(no_refresh=True, fetch_logs=True))
        self.assertEqual(calls, [])

    def test_a_rerun_invalidates_the_cache(self):
        self._finished("J1", host="eci3")
        self._run_with_scp(_JobsArgs(no_refresh=True, fetch_logs=True))
        # Requeued onto another worker: same job id, different attempt.
        ledger_mod.apply_state("J1", {"status": "failed", "host": "eci7",
                                      "finished_at": "2026-08-01 09:00:00"})
        _, _, calls = self._run_with_scp(_JobsArgs(no_refresh=True, fetch_logs=True))
        self.assertEqual(calls, [("J1", "eci7")])

    def test_running_jobs_are_refetched_every_time(self):
        self._finished("J1", status="running")
        self._run_with_scp(_JobsArgs(no_refresh=True, fetch_logs=True))
        _, _, calls = self._run_with_scp(_JobsArgs(no_refresh=True, fetch_logs=True))
        self.assertEqual(calls, [("J1", "eci7")])

    def test_a_vanished_remote_log_is_remembered_and_not_retried(self):
        self._finished("J1")
        _, err, _ = self._run_with_scp(
            _JobsArgs(no_refresh=True, fetch_logs=True),
            returncode=1, stderr="scp: /home/ubuntu/manager_jobs/J1.log: No such file or directory",
        )
        self.assertIn("no log left on eci7", err)
        _, _, calls = self._run_with_scp(_JobsArgs(no_refresh=True, fetch_logs=True))
        self.assertEqual(calls, [], "a known-missing log must not be re-requested")

    def test_a_transfer_error_is_warned_but_retried_next_time(self):
        self._finished("J1")
        _, err, _ = self._run_with_scp(
            _JobsArgs(no_refresh=True, fetch_logs=True),
            returncode=255, stderr="ssh: Could not resolve hostname eci7",
        )
        self.assertIn("could not fetch log", err)
        _, _, calls = self._run_with_scp(_JobsArgs(no_refresh=True, fetch_logs=True))
        self.assertEqual(calls, [("J1", "eci7")])

    def test_fetching_is_scoped_to_the_displayed_rows_not_the_whole_ledger(self):
        for i in range(5):
            self._finished(f"J{i}")
        _, _, calls = self._run_with_scp(_JobsArgs(no_refresh=True, fetch_logs=True, limit=2))
        self.assertEqual(len(calls), 2)

    def test_queued_jobs_with_no_worker_are_skipped(self):
        self._submit("QUEUED")
        ledger_mod.apply_state("QUEUED", {"status": "queued", "queue_position": 1})
        _, _, calls = self._run_with_scp(_JobsArgs(no_refresh=True, fetch_logs=True))
        self.assertEqual(calls, [])

    def test_no_log_line_rendered_when_nothing_was_fetched(self):
        self._finished("J1")
        out, _, _ = self._run_with_scp(_JobsArgs(no_refresh=True))
        self.assertNotIn("log: ", out)

    def test_log_flag_prints_a_bare_path_for_piping(self):
        self._finished("J1")
        out, _, calls = self._run_with_scp(_JobsArgs(log="J1"))
        self.assertEqual(calls, [("J1", "eci7")])
        self.assertEqual(out.strip(), str(logs_mod.local_log_path("J1", root=self.logroot)))

    def test_log_flag_accepts_a_prefix(self):
        self._finished("20260731-181013-cfd2e8")
        out, _, _ = self._run_with_scp(_JobsArgs(log="20260731-1810"))
        self.assertIn("20260731-181013-cfd2e8.log", out)

    def test_cat_prints_the_log_contents(self):
        self._finished("J1")
        out, _, calls = self._run_with_scp(_JobsArgs(cat="J1"), body="line one\nline two\n")
        self.assertEqual(calls, [("J1", "eci7")])
        self.assertEqual(out, "line one\nline two\n")

    def test_cat_uses_the_cache_on_a_second_call(self):
        self._finished("J1")
        self._run_with_scp(_JobsArgs(cat="J1"), body="cached body\n")
        out, _, calls = self._run_with_scp(_JobsArgs(cat="J1"))
        self.assertEqual(calls, [])
        self.assertEqual(out, "cached body\n")

    def test_cat_accepts_a_prefix(self):
        self._finished("20260731-181013-cfd2e8")
        out, _, _ = self._run_with_scp(_JobsArgs(cat="20260731-1810"), body="hello\n")
        self.assertEqual(out, "hello\n")

    def test_cat_emits_nothing_but_the_log_on_stdout(self):
        """So `--cat X > file` and `--cat X | grep` behave."""
        self._finished("J1")
        out, _, _ = self._run_with_scp(_JobsArgs(cat="J1"), body="only this\n")
        self.assertNotIn("SUBMITTED", out)
        self.assertNotIn("J1  ", out)

    def test_cat_handles_a_large_log(self):
        self._finished("J1")
        body = "".join(f"line {i}\n" for i in range(50000))
        out, _, _ = self._run_with_scp(_JobsArgs(cat="J1"), body=body)
        self.assertEqual(out, body)

    def test_cat_tolerates_undecodable_bytes(self):
        """A job's stdout is whatever it wrote; that's no reason to refuse to show it."""
        self._finished("J1")
        self._run_with_scp(_JobsArgs(no_refresh=True, fetch_logs=True), body="x")
        logs_mod.local_log_path("J1", root=self.logroot).write_bytes(b"before \xff\xfe after\n")
        out, _, _ = self._run_with_scp(_JobsArgs(cat="J1"))
        self.assertIn("before", out)
        self.assertIn("after", out)

    def test_cat_on_an_unknown_job_exits_1(self):
        with self.assertRaises(SystemExit) as ctx:
            self._run_with_scp(_JobsArgs(cat="NOPE"))
        self.assertEqual(ctx.exception.code, 1)

    def test_cat_on_a_vanished_remote_log_exits_1(self):
        self._finished("J1")
        with self.assertRaises(SystemExit) as ctx:
            self._run_with_scp(
                _JobsArgs(cat="J1"), returncode=1,
                stderr="scp: /home/ubuntu/manager_jobs/J1.log: No such file or directory",
            )
        self.assertEqual(ctx.exception.code, 1)

    def test_parser_rejects_log_and_cat_together(self):
        with self.assertRaises(SystemExit):
            client_cli.build_parser().parse_args(["jobs", "--log", "A", "--cat", "A"])

    def test_parser_wires_cat(self):
        args = client_cli.build_parser().parse_args(["jobs", "--cat", "JOB-A"])
        self.assertEqual(args.cat, "JOB-A")
        self.assertIsNone(args.log)

    def test_log_flag_on_an_unknown_job_exits_1(self):
        with self.assertRaises(SystemExit) as ctx:
            self._run_with_scp(_JobsArgs(log="NOPE"))
        self.assertEqual(ctx.exception.code, 1)

    def test_log_flag_on_a_job_with_no_worker_exits_1(self):
        self._submit("QUEUED")
        with self.assertRaises(SystemExit) as ctx:
            self._run_with_scp(_JobsArgs(log="QUEUED"))
        self.assertEqual(ctx.exception.code, 1)

    def test_missing_scp_warns_once_and_does_not_crash(self):
        self._finished("J1")
        out, err = io.StringIO(), io.StringIO()
        with patch.object(logs_mod, "scp_available", return_value=False), \
             patch("awsqueueengine.client.cli.effective_queue_host", return_value=None):
            with redirect_stdout(out), redirect_stderr(err):
                client_cli.cmd_jobs(_JobsArgs(no_refresh=True, fetch_logs=True))
        self.assertIn("not found on PATH", err.getvalue())
        self.assertIn("J1", out.getvalue())

    def test_forget_deletes_the_cached_log(self):
        self._finished("J1")
        self._run_with_scp(_JobsArgs(no_refresh=True, fetch_logs=True))
        cached = logs_mod.local_log_path("J1", root=self.logroot)
        self.assertTrue(cached.exists())
        self._run(_JobsArgs(forget=["J1"]))
        self.assertFalse(cached.exists(), "a forgotten job must not leave an orphan log")


class SubmitLedgerTests(_LedgerFixture):
    """What `submit` writes into the ledger. (The subprocess end of this, which
    proves ~/.awsqe/client/jobs.json resolves for real, lives in test_cli_submit.)"""

    class _SubmitArgs:
        payload = None
        hosts_file = None
        hosts = None
        host_set = None
        queue = None
        priority = None
        high_priority = False
        preempt = False
        mps = False
        array = None
        queue_host = "queuebox"

    def _submit_remote(self, **overrides):
        args = self._SubmitArgs()
        for key, value in overrides.items():
            setattr(args, key, value)
        with patch("awsqueueengine.client.cli.rpc_call", return_value={"job_id": "JOB-1"}), \
             patch("awsqueueengine.client.cli.archive_payload_to_temp") as archive, \
             patch("awsqueueengine.client.cli.upload_payload_archive_to_s3",
                   return_value="s3://bucket/key.tar.gz"), \
             patch("awsqueueengine.client.cli.sizeof_local_path_bytes", return_value=10):
            archive.return_value = self.root_tmp / "archive.tar.gz"
            archive.return_value.write_text("")
            with redirect_stdout(io.StringIO()):
                client_cli.cmd_submit_remote(args, "python train.py")
        return ledger_mod.load_ledger()

    @property
    def root_tmp(self):
        return Path(self.tmpdir.name)

    def test_relative_payload_path_is_stored_absolute(self):
        """The row has to make sense from any cwd, not just the one that submitted."""
        payload = self.root_tmp / "run"
        payload.mkdir()
        import os
        original = os.getcwd()
        os.chdir(self.root_tmp)
        try:
            records = self._submit_remote(payload="run")
        finally:
            os.chdir(original)
        self.assertTrue(Path(records[0]["payload"]).is_absolute())
        self.assertEqual(Path(records[0]["payload"]).name, "run")

    def test_tilde_in_the_payload_path_is_expanded(self):
        with patch.object(Path, "home", return_value=self.root_tmp):
            records = self._submit_remote(payload="~/data")
        self.assertNotIn("~", records[0]["payload"])

    def test_the_server_assigned_job_id_is_what_gets_tracked(self):
        records = self._submit_remote()
        self.assertEqual(records[0]["job_id"], "JOB-1")
        self.assertEqual(records[0]["cmd"], "python train.py")
        self.assertEqual(records[0]["queue_host"], "queuebox")

    def test_array_tag_is_recorded(self):
        records = self._submit_remote(array="ffpopt-IDC")
        self.assertEqual(records[0]["array_id"], "ffpopt-IDC")

    def test_untagged_submit_records_an_empty_tag(self):
        records = self._submit_remote()
        self.assertEqual(records[0]["array_id"], "")

    def test_array_tag_is_sent_to_the_queue_host_too(self):
        """Sent from day one so a batch submitted before the host understands
        `array_id` is still correct on the wire, not permanently untaggable."""
        args = self._SubmitArgs()
        args.array = "ffpopt-IDC"
        with patch("awsqueueengine.client.cli.rpc_call",
                   return_value={"job_id": "JOB-1"}) as rpc:
            with redirect_stdout(io.StringIO()):
                client_cli.cmd_submit_remote(args, "python train.py")
        self.assertEqual(rpc.call_args[0][2]["array_id"], "ffpopt-IDC")

    def test_no_array_id_param_is_sent_when_untagged(self):
        args = self._SubmitArgs()
        with patch("awsqueueengine.client.cli.rpc_call",
                   return_value={"job_id": "JOB-1"}) as rpc:
            with redirect_stdout(io.StringIO()):
                client_cli.cmd_submit_remote(args, "python train.py")
        self.assertNotIn("array_id", rpc.call_args[0][2])

    def test_a_bad_array_name_is_rejected_before_anything_is_uploaded(self):
        """The name is checked first: a typo must not cost a tar-and-push to S3."""
        args = self._SubmitArgs()
        args.array = "ffpopt IDC"
        args.payload = str(self.root_tmp)
        with patch("awsqueueengine.client.cli.rpc_call") as rpc, \
             patch("awsqueueengine.client.cli.archive_payload_to_temp") as archive:
            out = io.StringIO()
            with self.assertRaises(SystemExit) as ctx, redirect_stdout(out):
                client_cli.cmd_submit_remote(args, "python train.py")
        self.assertEqual(ctx.exception.code, 2)
        self.assertIn("--array", out.getvalue())
        archive.assert_not_called()
        rpc.assert_not_called()
        self.assertEqual(ledger_mod.load_ledger(), [])


class QdelLedgerIntegrationTests(_LedgerFixture):
    def test_qdel_marks_removed_jobs_deleted(self):
        self._submit("A")
        self._submit("B")

        class Args:
            queue_host = "qh1"
            job_ids = ["A"]
            index = []
            queue = None

        with patch("awsqueueengine.client.cli.validate_qdel_selectors", return_value=None), \
             patch("awsqueueengine.client.cli.qdel_selectors", return_value=(["A"], [], None)), \
             patch("awsqueueengine.client.cli.rpc_call",
                   return_value={"removed": [{"index": 1, "item": {"job_id": "A", "cmd": "x"}}]}):
            with redirect_stdout(io.StringIO()):
                client_cli.cmd_qdel_remote(Args())

        by_id = {r["job_id"]: r for r in ledger_mod.load_ledger()}
        self.assertEqual(by_id["A"]["status"], "deleted")
        self.assertEqual(by_id["B"]["status"], "submitted")

    def test_a_deleted_job_is_terminal_so_refresh_skips_it(self):
        self._submit("A")
        ledger_mod.mark_status(["A"], "deleted")
        out, _ = self._run(_JobsArgs())   # rpc=None raises if called
        self.assertIn("deleted", out)


if __name__ == "__main__":
    unittest.main()
