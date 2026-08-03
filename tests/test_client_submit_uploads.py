"""Payload preparation and the parallel upload pool behind `--payload-glob`."""
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from awsqueueengine.client import submit as submit_mod


class _PayloadFixture(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _payload(self, name):
        directory = self.root / name
        directory.mkdir()
        (directory / "run.py").write_text(f"# {name}\n")
        return directory


class PreparePayloadTests(_PayloadFixture):
    def test_returns_the_uri_and_size(self):
        payload = self._payload("one")
        with patch.object(submit_mod, "upload_payload_archive_to_s3",
                          return_value="s3://b/one.tar.gz"):
            result = submit_mod.prepare_payload(payload, bucket="b", prefix="p")
        self.assertEqual(result["s3_uri"], "s3://b/one.tar.gz")
        self.assertGreater(result["size_bytes"], 0)

    def test_the_temp_archive_is_removed_on_success(self):
        payload = self._payload("one")
        seen = {}

        def capture(archive_path, name, **kwargs):
            seen["path"] = Path(archive_path)
            self.assertTrue(seen["path"].exists())
            return "s3://b/k.tar.gz"

        with patch.object(submit_mod, "upload_payload_archive_to_s3", side_effect=capture):
            submit_mod.prepare_payload(payload, bucket="b", prefix="p")
        self.assertFalse(seen["path"].exists())

    def test_the_temp_archive_is_removed_when_the_upload_raises(self):
        payload = self._payload("one")
        seen = {}

        def boom(archive_path, name, **kwargs):
            seen["path"] = Path(archive_path)
            raise RuntimeError("s3 down")

        with patch.object(submit_mod, "upload_payload_archive_to_s3", side_effect=boom):
            with self.assertRaises(RuntimeError):
                submit_mod.prepare_payload(payload, bucket="b", prefix="p")
        self.assertFalse(seen["path"].exists())

    def test_a_missing_payload_raises(self):
        with self.assertRaises(FileNotFoundError):
            submit_mod.prepare_payload(self.root / "nope", bucket="b", prefix="p")


class UploadPayloadsParallelTests(_PayloadFixture):
    def test_results_stay_positional_regardless_of_completion_order(self):
        """Each result has to pair with the job id minted for its payload, so
        the order must be the input's, not the order uploads finished in."""
        payloads = [self._payload(f"IDC{i}") for i in range(4)]

        def upload(archive_path, name, **kwargs):
            # Finish in reverse order, so a result list built from completion
            # order would come back backwards.
            time.sleep(0.02 * (4 - int(name[-1])))
            return f"s3://b/{name}.tar.gz"

        with patch.object(submit_mod, "upload_payload_archive_to_s3", side_effect=upload):
            results = submit_mod.upload_payloads_parallel(
                payloads, bucket="b", prefix="p", max_workers=4,
            )
        self.assertEqual([r["s3_uri"] for r in results],
                         [f"s3://b/IDC{i}.tar.gz" for i in range(4)])

    def test_a_failure_is_returned_in_place_rather_than_abandoning_the_rest(self):
        payloads = [self._payload(f"IDC{i}") for i in range(4)]

        def upload(archive_path, name, **kwargs):
            if name == "IDC2":
                raise RuntimeError("upload exploded")
            return f"s3://b/{name}.tar.gz"

        with patch.object(submit_mod, "upload_payload_archive_to_s3", side_effect=upload):
            results = submit_mod.upload_payloads_parallel(payloads, bucket="b", prefix="p")

        self.assertIsInstance(results[2], RuntimeError)
        self.assertEqual([type(r) for r in results].count(RuntimeError), 1)
        for index in (0, 1, 3):
            self.assertEqual(results[index]["s3_uri"], f"s3://b/IDC{index}.tar.gz")

    def test_concurrency_never_exceeds_the_cap(self):
        payloads = [self._payload(f"IDC{i}") for i in range(8)]
        lock = threading.Lock()
        state = {"live": 0, "peak": 0}
        gate = threading.Event()

        def upload(archive_path, name, **kwargs):
            with lock:
                state["live"] += 1
                state["peak"] = max(state["peak"], state["live"])
            # Hold every worker until the peak has had a chance to build, so
            # this measures the cap rather than how fast the machine is.
            gate.wait(timeout=1.0)
            with lock:
                state["live"] -= 1
            return "s3://b/k.tar.gz"

        with patch.object(submit_mod, "upload_payload_archive_to_s3", side_effect=upload):
            worker = threading.Thread(
                target=submit_mod.upload_payloads_parallel,
                args=(payloads,),
                kwargs={"bucket": "b", "prefix": "p", "max_workers": 3},
            )
            worker.start()
            # Give the pool time to saturate, then release.
            worker.join(timeout=0.5)
            gate.set()
            worker.join(timeout=5)

        self.assertLessEqual(state["peak"], 3)
        self.assertGreater(state["peak"], 1, "the pool never actually ran in parallel")

    def test_the_cap_never_exceeds_the_work_available(self):
        payloads = [self._payload("only")]
        with patch.object(submit_mod, "upload_payload_archive_to_s3",
                          return_value="s3://b/k.tar.gz"):
            self.assertEqual(
                len(submit_mod.upload_payloads_parallel(
                    payloads, bucket="b", prefix="p", max_workers=64)),
                1,
            )

    def test_no_payloads_is_a_no_op(self):
        self.assertEqual(
            submit_mod.upload_payloads_parallel([], bucket="b", prefix="p"), [],
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
