"""Unit tests for shared/protocol.py and shared/rpc_client.py.

Uses an injected transport so we never actually run ssh.
"""
import json
import subprocess
import unittest

from awsqueueengine.shared.protocol import (
    PROTOCOL_VERSION,
    RpcError,
    RpcTransportError,
    make_error,
    make_ok,
    make_request,
)
from awsqueueengine.shared.rpc_client import call as rpc_call


class FakeProc:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class ProtocolEnvelopeTests(unittest.TestCase):
    def test_make_request_carries_version_method_params(self):
        req = make_request("list", {"foo": 1})
        self.assertEqual(req, {"version": PROTOCOL_VERSION, "method": "list", "params": {"foo": 1}})

    def test_make_request_defaults_params_to_empty_object(self):
        req = make_request("qstat")
        self.assertEqual(req["params"], {})

    def test_make_ok_and_make_error_envelopes(self):
        ok = make_ok({"jobs": []})
        self.assertEqual(ok, {"version": PROTOCOL_VERSION, "ok": True, "result": {"jobs": []}})
        err = make_error("not_found", "no such job")
        self.assertEqual(err, {
            "version": PROTOCOL_VERSION, "ok": False,
            "error": {"code": "not_found", "message": "no such job"},
        })


class RpcClientCallTests(unittest.TestCase):
    def _transport_returning(self, response_dict, returncode=0, stderr=""):
        captured = {}

        def transport(argv, stdin_text, timeout):
            captured["argv"] = list(argv)
            captured["stdin"] = stdin_text
            captured["timeout"] = timeout
            return FakeProc(returncode=returncode, stdout=json.dumps(response_dict), stderr=stderr)

        return transport, captured

    def test_successful_call_returns_result_and_sends_correct_request(self):
        transport, captured = self._transport_returning({
            "version": PROTOCOL_VERSION, "ok": True, "result": {"jobs": [{"job_id": "x"}]},
        })
        result = rpc_call("host1", "list", {"page": 2}, transport=transport)
        self.assertEqual(result, {"jobs": [{"job_id": "x"}]})
        self.assertEqual(captured["argv"][1:], ["host1", "awsqe-host", "rpc"])
        request = json.loads(captured["stdin"])
        self.assertEqual(request, {"version": PROTOCOL_VERSION, "method": "list", "params": {"page": 2}})

    def test_application_error_raises_RpcError_with_code_and_message(self):
        transport, _ = self._transport_returning({
            "version": PROTOCOL_VERSION, "ok": False,
            "error": {"code": "not_found", "message": "job X not found"},
        })
        with self.assertRaises(RpcError) as ctx:
            rpc_call("host1", "job_info", {"job_id": "X"}, transport=transport)
        self.assertEqual(ctx.exception.code, "not_found")
        self.assertEqual(ctx.exception.message, "job X not found")

    def test_nonzero_exit_raises_RpcTransportError(self):
        def transport(argv, stdin_text, timeout):
            return FakeProc(returncode=255, stdout="", stderr="ssh: Could not resolve hostname")
        with self.assertRaises(RpcTransportError) as ctx:
            rpc_call("nope", "list", transport=transport)
        self.assertEqual(ctx.exception.returncode, 255)
        self.assertIn("Could not resolve hostname", ctx.exception.detail)

    def test_non_json_response_raises_RpcTransportError(self):
        def transport(argv, stdin_text, timeout):
            return FakeProc(returncode=0, stdout="this isn't JSON\n")
        with self.assertRaises(RpcTransportError) as ctx:
            rpc_call("host1", "list", transport=transport)
        self.assertIn("non-JSON response", ctx.exception.detail)

    def test_wrong_version_in_response_raises_RpcTransportError(self):
        def transport(argv, stdin_text, timeout):
            return FakeProc(returncode=0, stdout=json.dumps({"version": 99, "ok": True, "result": {}}))
        with self.assertRaises(RpcTransportError) as ctx:
            rpc_call("host1", "list", transport=transport)
        self.assertIn("bad response envelope", ctx.exception.detail)

    def test_timeout_raises_RpcTransportError(self):
        def transport(argv, stdin_text, timeout):
            raise subprocess.TimeoutExpired(cmd=argv, timeout=timeout)
        with self.assertRaises(RpcTransportError) as ctx:
            rpc_call("host1", "list", transport=transport, timeout=7)
        self.assertEqual(ctx.exception.returncode, 124)
        self.assertIn("timeout", ctx.exception.detail)


if __name__ == "__main__":
    unittest.main()
