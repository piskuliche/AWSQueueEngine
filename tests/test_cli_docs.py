"""Every user-facing CLI flag and subcommand must appear in the docs.

The other two gates cover structure, not prose: `make -C docs coverage` catches
an undocumented *module*, and test_protocol_docs catches an undocumented *RPC
method*. Neither noticed when `--payload-glob` shipped with #19 and went
undocumented for a release — the docs were ported from a README that predated
the flag, and nothing compared the two.

This closes that gap for the surface users actually type.
"""
import pathlib
import re
import unittest

from awsqueueengine.client.cli import build_parser as build_client_parser
from awsqueueengine.host.cli import build_parser as build_host_parser

DOCS = pathlib.Path(__file__).resolve().parents[1] / "docs"

#: Flags deliberately left out of the prose docs, with the reason. These are
#: wire-level plumbing the client sets on the user's behalf — documenting them
#: under `awsqe-host submit` would invite people to hand-set them.
EXEMPT = {
    "--payload-s3-uri": "set by the client after upload; not typed by hand",
    "--payload-size-bytes": "set by the client after upload; not typed by hand",
}


def _docs_text() -> str:
    return " ".join(
        p.read_text()
        for p in DOCS.rglob("*.rst")
        if "_build" not in p.parts
    )


def _subcommands_and_flags():
    """Return ({subcommand: [cli, ...]}, {flag: [cli subcommand, ...]})."""
    subs: dict[str, set[str]] = {}
    flags: dict[str, set[str]] = {}
    for parser, cli in ((build_client_parser(), "awsqe-client"),
                        (build_host_parser(), "awsqe-host")):
        for action in parser._actions:
            for name, sub in (getattr(action, "_name_parser_map", None) or {}).items():
                subs.setdefault(name, set()).add(cli)
                for sub_action in sub._actions:
                    for opt in sub_action.option_strings:
                        if opt.startswith("--") and opt != "--help":
                            flags.setdefault(opt, set()).add(f"{cli} {name}")
    return subs, flags


class CliDocsTests(unittest.TestCase):
    def test_every_subcommand_is_documented(self):
        subs, _ = _subcommands_and_flags()
        text = _docs_text()
        missing = sorted(name for name in subs if f"``{name}``" not in text)
        self.assertEqual([], missing, f"subcommands absent from docs/: {missing}")

    def test_every_flag_is_documented(self):
        _, flags = _subcommands_and_flags()
        text = _docs_text()
        missing = {
            flag: sorted(where)
            for flag, where in flags.items()
            if flag not in EXEMPT and flag not in text
        }
        self.assertEqual(
            {},
            missing,
            "flags absent from docs/ — document them, or add to EXEMPT with a "
            f"reason if they are internal: {missing}",
        )

    def test_exemptions_still_exist(self):
        """A stale exemption would silently excuse a flag that got renamed."""
        _, flags = _subcommands_and_flags()
        stale = sorted(set(EXEMPT) - set(flags))
        self.assertEqual([], stale, f"EXEMPT lists flags that no longer exist: {stale}")


if __name__ == "__main__":
    unittest.main()
