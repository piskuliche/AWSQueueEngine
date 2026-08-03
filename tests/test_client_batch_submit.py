"""`awsqe-client submit --payload-glob`: one process, one job per directory.

The things worth holding: nothing is rolled back on a partial failure (by then
the successes are real jobs), job ids follow directory order even though the
uploads finish out of order, `--dry-run` touches neither S3 nor the network, and
the whole batch costs one ledger write and one SSH round trip.
"""
import io
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from awsqueueengine.client import cli as client_cli
from awsqueueengine.client import ledger as ledger_mod
from awsqueueengine.shared.protocol import RpcError, RpcTransportError


class _BatchArgs:
    """The attribute surface `cmd_submit_batch` reads off argparse."""

    def __init__(self, **kwargs):
        self.queue_host = "queuebox"
        self.hosts_file = None
        self.payload = None
        self.payload_glob = "IDC*"
        self.hosts = None
        self.host_set = None
        self.queue = "production"
        self.priority = None
        self.high_priority = False
        self.preempt = False
        self.mps = False
        self.array = None
        self.jobs = None
        self.dry_run = False
        self.__dict__.update(kwargs)


class _BatchFixture(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self.ledger_path = self.root / "jobs.json"
        self._original = ledger_mod.LEDGER_PATH
        ledger_mod.LEDGER_PATH = self.ledger_path
        self._cwd = os.getcwd()
        os.chdir(self.root)

    def tearDown(self):
        os.chdir(self._cwd)
        ledger_mod.LEDGER_PATH = self._original
        self.tmpdir.cleanup()

    def _payloads(self, count):
        for i in range(1, count + 1):
            directory = self.root / f"IDC{i}"
            directory.mkdir()
            (directory / "run.py").write_text(f"# {i}\n")

    def _fake_upload(self, paths, **kwargs):
        return [{"s3_uri": f"s3://b/{p.name}.tar.gz", "size_bytes": 10} for p in paths]

    def _run(self, args=None, *, upload=None, rpc=None):
        """Run cmd_submit_batch with S3 and the network faked out."""
        calls = {"rpc": [], "upload_kwargs": []}

        def default_rpc(host, method, params, **kwargs):
            calls["rpc"].append((method, params))
            return {
                "results": [
                    {"index": i, "ok": True, "job_id": job["job_id"],
                     "queue": job["queue"], "hosts": job.get("hosts"),
                     "array_id": job.get("array_id")}
                    for i, job in enumerate(params["jobs"])
                ],
                "enqueued": len(params["jobs"]),
                "skipped": [],
            }

        def tracking_upload(paths, **kwargs):
            calls["upload_kwargs"].append(kwargs)
            return (upload or self._fake_upload)(paths, **kwargs)

        out, err = io.StringIO(), io.StringIO()
        calls["exit_code"] = None
        with patch("awsqueueengine.client.cli.rpc_call", side_effect=rpc or default_rpc), \
             patch("awsqueueengine.client.cli.upload_payloads_parallel",
                   side_effect=tracking_upload), \
             patch("awsqueueengine.client.cli.load_config", return_value={}), \
             patch("awsqueueengine.client.cli.effective_s3_bucket", return_value="bucket"), \
             patch("awsqueueengine.client.cli.effective_s3_prefix", return_value="p"):
            with redirect_stdout(out), redirect_stderr(err):
                # Captured rather than propagated: the failure paths print
                # before they exit, and that output is what is under test.
                try:
                    client_cli.cmd_submit_batch(args or _BatchArgs(), "python run.py")
                except SystemExit as exc:
                    calls["exit_code"] = exc.code
        return out.getvalue(), err.getvalue(), calls


class GlobResolutionTests(_BatchFixture):
    def test_matches_are_sorted(self):
        self._payloads(3)
        matches = client_cli._resolve_payload_glob("IDC*")
        self.assertEqual([p.name for p in matches], ["IDC1", "IDC2", "IDC3"])

    def test_no_match_exits_rather_than_submitting_nothing_quietly(self):
        out = io.StringIO()
        with self.assertRaises(SystemExit) as ctx, redirect_stdout(out):
            client_cli._resolve_payload_glob("NOPE*")
        self.assertEqual(ctx.exception.code, 1)
        self.assertIn("matched no directories", out.getvalue())

    def test_files_are_not_payloads_and_are_reported_when_they_are_all_there_is(self):
        (self.root / "IDC1").write_text("not a directory")
        out = io.StringIO()
        with self.assertRaises(SystemExit), redirect_stdout(out):
            client_cli._resolve_payload_glob("IDC*")
        self.assertIn("1 non-directory match(es) ignored", out.getvalue())

    def test_files_alongside_directories_are_skipped_silently(self):
        self._payloads(2)
        (self.root / "IDC-notes.txt").write_text("x")
        matches = client_cli._resolve_payload_glob("IDC*")
        self.assertEqual([p.name for p in matches], ["IDC1", "IDC2"])


class JobIdTests(unittest.TestCase):
    def test_ids_are_distinct_even_when_the_tag_source_repeats(self):
        """`new_job_tag` is a second-resolution stamp plus six hex characters;
        105 minted inside one second carries a real birthday collision, and a
        collision would put two queue entries under one id."""
        with patch("awsqueueengine.client.cli.new_job_tag",
                   side_effect=["A", "A", "A", "B", "C"]):
            self.assertEqual(client_cli._mint_job_ids(3), ["A", "B", "C"])

    def test_mints_exactly_the_requested_count(self):
        self.assertEqual(len(client_cli._mint_job_ids(7)), 7)
        self.assertEqual(client_cli._mint_job_ids(0), [])


class ParserAndDispatchTests(unittest.TestCase):
    def _parse(self, argv):
        return client_cli.build_parser().parse_args(argv)

    def test_batch_flags_parse(self):
        args = self._parse(["submit", "--payload-glob", "IDC*", "-j", "8",
                            "--dry-run", "python", "run.py"])
        self.assertEqual(args.payload_glob, "IDC*")
        self.assertEqual(args.jobs, 8)
        self.assertTrue(args.dry_run)

    def test_defaults(self):
        args = self._parse(["submit", "python", "run.py"])
        self.assertIsNone(args.payload_glob)
        self.assertIsNone(args.jobs)
        self.assertFalse(args.dry_run)

    def test_payload_and_payload_glob_cannot_be_combined(self):
        args = self._parse(["submit", "--payload", "x", "--payload-glob", "IDC*", "run"])
        out = io.StringIO()
        with patch("awsqueueengine.client.cli.cmd_submit_batch") as batch, \
             patch("awsqueueengine.client.cli.cmd_submit_remote") as single:
            with self.assertRaises(SystemExit) as ctx, redirect_stdout(out):
                client_cli.dispatch(args)
        self.assertEqual(ctx.exception.code, 2)
        self.assertIn("cannot be combined", out.getvalue())
        batch.assert_not_called()
        single.assert_not_called()

    def test_dry_run_without_a_glob_is_rejected_rather_than_ignored(self):
        args = self._parse(["submit", "--dry-run", "python", "run.py"])
        out = io.StringIO()
        with patch("awsqueueengine.client.cli.cmd_submit_remote") as single:
            with self.assertRaises(SystemExit) as ctx, redirect_stdout(out):
                client_cli.dispatch(args)
        self.assertEqual(ctx.exception.code, 2)
        single.assert_not_called()

    def test_a_glob_routes_to_the_batch_handler(self):
        args = self._parse(["submit", "--payload-glob", "IDC*", "python", "run.py"])
        with patch("awsqueueengine.client.cli.cmd_submit_batch") as batch, \
             patch("awsqueueengine.client.cli.cmd_submit_remote") as single, \
             patch("awsqueueengine.client.cli._resolve_queue_host"):
            client_cli.dispatch(args)
        batch.assert_called_once()
        single.assert_not_called()

    def test_no_glob_still_routes_to_the_single_handler(self):
        args = self._parse(["submit", "python", "run.py"])
        with patch("awsqueueengine.client.cli.cmd_submit_batch") as batch, \
             patch("awsqueueengine.client.cli.cmd_submit_remote") as single, \
             patch("awsqueueengine.client.cli._resolve_queue_host"):
            client_cli.dispatch(args)
        single.assert_called_once()
        batch.assert_not_called()


class DryRunTests(_BatchFixture):
    def test_dry_run_touches_neither_s3_nor_the_network_nor_the_ledger(self):
        self._payloads(3)
        out, _, calls = self._run(_BatchArgs(dry_run=True))
        self.assertEqual(calls["rpc"], [])
        self.assertEqual(calls["upload_kwargs"], [])
        self.assertEqual(ledger_mod.load_ledger(), [])
        self.assertIn("Would submit 3 job(s)", out)

    def test_dry_run_lists_each_payload_with_the_id_it_would_get(self):
        self._payloads(2)
        out, _, _ = self._run(_BatchArgs(dry_run=True))
        self.assertIn("IDC1", out)
        self.assertIn("IDC2", out)
        self.assertIn("python run.py", out)


class BatchSubmitTests(_BatchFixture):
    def test_one_rpc_round_trip_for_the_whole_batch(self):
        self._payloads(5)
        _, _, calls = self._run()
        self.assertEqual(len(calls["rpc"]), 1)
        method, params = calls["rpc"][0]
        self.assertEqual(method, "enqueue_many")
        self.assertEqual(len(params["jobs"]), 5)

    def test_job_ids_follow_directory_order(self):
        self._payloads(5)
        _, _, calls = self._run()
        jobs = calls["rpc"][0][1]["jobs"]
        # Minted up front, so each payload keeps the id it was assigned even
        # though the uploads finish out of order. (Ids are a timestamp plus
        # random hex, so they are ordered by assignment, not lexically.)
        self.assertEqual([job["payload_s3_uri"] for job in jobs],
                         [f"s3://b/IDC{i}.tar.gz" for i in range(1, 6)])
        self.assertEqual(len({job["job_id"] for job in jobs}), 5)

    def test_the_whole_batch_costs_one_ledger_write(self):
        self._payloads(5)
        with patch("awsqueueengine.client.ledger.save_ledger",
                   wraps=ledger_mod.save_ledger) as save:
            self._run()
        self.assertEqual(save.call_count, 1)

    def test_every_job_is_tracked_under_one_derived_array_tag(self):
        self._payloads(4)
        self._run()
        records = ledger_mod.load_ledger()
        self.assertEqual(len(records), 4)
        tags = {r["array_id"] for r in records}
        self.assertEqual(len(tags), 1)
        self.assertTrue(tags.pop().startswith("IDC-"))

    def test_an_explicit_array_name_overrides_the_derived_one(self):
        self._payloads(2)
        self._run(_BatchArgs(array="ffpopt-IDC"))
        self.assertEqual({r["array_id"] for r in ledger_mod.load_ledger()}, {"ffpopt-IDC"})

    def test_a_bad_array_name_is_rejected_before_any_upload(self):
        self._payloads(2)
        _, _, calls = self._run(_BatchArgs(array="ffpopt IDC"))
        self.assertEqual(calls["exit_code"], 2)
        self.assertEqual(calls["upload_kwargs"], [])
        self.assertEqual(ledger_mod.load_ledger(), [])

    def test_array_size_is_recorded_because_this_path_knows_it(self):
        """A shell loop never knows its own size; one invocation does, which is
        what lets the grouped view say 130/142 after eviction."""
        self._payloads(3)
        self._run()
        self.assertEqual({r["array_size"] for r in ledger_mod.load_ledger()}, {3})

    def test_payload_paths_are_recorded_absolute(self):
        self._payloads(1)
        self._run()
        self.assertTrue(Path(ledger_mod.load_ledger()[0]["payload"]).is_absolute())

    def test_submit_flags_are_forwarded_to_every_job(self):
        self._payloads(3)
        _, _, calls = self._run(_BatchArgs(mps=True, preempt=True, priority=-100,
                                           hosts=["eci17"], queue="production"))
        for job in calls["rpc"][0][1]["jobs"]:
            self.assertTrue(job["mps"])
            self.assertTrue(job["preempt"])
            self.assertEqual(job["priority"], -100)
            self.assertEqual(job["hosts"], ["eci17"])
            self.assertEqual(job["queue"], "production")

    def test_high_priority_alias_is_forwarded(self):
        self._payloads(1)
        _, _, calls = self._run(_BatchArgs(high_priority=True))
        self.assertTrue(calls["rpc"][0][1]["jobs"][0]["high_priority"])

    def test_the_worker_cap_is_passed_through(self):
        self._payloads(3)
        _, _, calls = self._run(_BatchArgs(jobs=2))
        self.assertEqual(calls["upload_kwargs"][0]["max_workers"], 2)


class PartialFailureTests(_BatchFixture):
    def _upload_with_failure(self, failing_index):
        def upload(paths, **kwargs):
            return [
                RuntimeError("upload exploded") if i == failing_index
                else {"s3_uri": f"s3://b/{p.name}.tar.gz", "size_bytes": 10}
                for i, p in enumerate(paths)
            ]
        return upload

    def test_the_successes_are_still_submitted_and_never_rolled_back(self):
        self._payloads(5)
        _, _, calls = self._run(upload=self._upload_with_failure(2))
        # Non-zero so a driving script notices...
        self.assertEqual(calls["exit_code"], 1)
        # ...but the four that uploaded are real jobs and stay tracked.
        self.assertEqual(len(ledger_mod.load_ledger()), 4)

    def test_both_sets_are_reported_by_name(self):
        self._payloads(5)
        out, _, _ = self._run(upload=self._upload_with_failure(2))
        self.assertIn("Submitted 4 job(s)", out)
        self.assertIn("failed to upload and were NOT submitted", out)
        self.assertIn("IDC3", out)
        self.assertIn("upload exploded", out)

    def test_array_size_counts_only_what_was_actually_submitted(self):
        self._payloads(5)
        self._run(upload=self._upload_with_failure(0))
        self.assertEqual({r["array_size"] for r in ledger_mod.load_ledger()}, {4})

    def test_every_upload_failing_submits_nothing(self):
        self._payloads(3)

        def all_fail(paths, **kwargs):
            return [RuntimeError("no") for _ in paths]

        _, _, calls = self._run(upload=all_fail)
        self.assertEqual(calls["exit_code"], 1)
        self.assertEqual(ledger_mod.load_ledger(), [])

    def test_an_unreachable_host_says_the_payloads_are_already_in_s3(self):
        """Not a clean slate: the uploads happened, so a retry would re-upload."""
        self._payloads(3)

        def boom(host, method, params, **kwargs):
            raise RpcTransportError(255, "ssh died")

        _, err, calls = self._run(rpc=boom)
        self.assertEqual(calls["exit_code"], 1)
        self.assertIn("are in S3 but nothing was enqueued", err)
        self.assertEqual(ledger_mod.load_ledger(), [])


class OlderHostTests(_BatchFixture):
    def test_falls_back_to_one_enqueue_per_job(self):
        self._payloads(3)
        calls = []

        def rpc(host, method, params, **kwargs):
            calls.append(method)
            if method == "enqueue_many":
                raise RpcError("unknown_method", "no such method: enqueue_many")
            return {"job_id": params["job_id"], "queue": params["queue"]}

        _, err, _ = self._run(rpc=rpc)
        self.assertEqual(calls, ["enqueue_many", "enqueue", "enqueue", "enqueue"])
        self.assertIn("older awsqe-host", err)
        self.assertEqual(len(ledger_mod.load_ledger()), 3)

    def test_a_real_rpc_error_is_not_mistaken_for_an_old_host(self):
        self._payloads(2)

        def rpc(host, method, params, **kwargs):
            raise RpcError("invalid_params", "job 0: unknown queue 'nope'")

        _, _, calls = self._run(rpc=rpc)
        self.assertEqual(calls["exit_code"], 1)
        self.assertEqual(ledger_mod.load_ledger(), [])

    def test_a_host_with_a_smaller_cap_still_gets_the_whole_batch(self):
        self._payloads(4)
        seen = []

        def rpc(host, method, params, **kwargs):
            jobs = params["jobs"]
            take = jobs[:2]
            seen.append(len(jobs))
            return {
                "results": [{"index": i, "ok": True, "job_id": j["job_id"],
                             "queue": j["queue"], "hosts": None, "array_id": j["array_id"]}
                            for i, j in enumerate(take)],
                "enqueued": len(take),
                "skipped": list(range(len(take), len(jobs))),
            }

        self._run(rpc=rpc)
        self.assertEqual(seen, [4, 2])
        self.assertEqual(len(ledger_mod.load_ledger()), 4)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
