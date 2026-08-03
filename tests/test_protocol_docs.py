"""The protocol reference must list exactly the methods the host serves.

docs/protocol.rst is the contract other clients (notably the Android viewer)
are written against, and its method table is hand-written so the wording stays
under editorial control. This is what stops it drifting from METHODS.
"""
import re
import unittest
from pathlib import Path

from awsqueueengine.host.rpc import METHODS

PROTOCOL_RST = Path(__file__).resolve().parents[1] / "docs" / "protocol.rst"


class ProtocolDocsTests(unittest.TestCase):
    def test_every_rpc_method_is_in_the_protocol_reference(self):
        text = PROTOCOL_RST.read_text()
        # Each row links its handler as :func:`~awsqueueengine.host.rpc.handle_x`.
        documented = set(re.findall(r"awsqueueengine\.host\.rpc\.(handle_\w+)", text))
        expected = {handler.__name__ for handler in METHODS.values()}

        self.assertEqual(
            expected,
            documented,
            "docs/protocol.rst is out of sync with METHODS. Undocumented: "
            f"{sorted(expected - documented)}; stale: {sorted(documented - expected)}",
        )

    def test_every_method_name_appears_verbatim(self):
        text = PROTOCOL_RST.read_text()
        missing = [name for name in METHODS if f"``{name}``" not in text]
        self.assertEqual([], missing, f"method names missing from the table: {missing}")


if __name__ == "__main__":
    unittest.main()
