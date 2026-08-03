#!/usr/bin/env python3
"""Assert every package module appears in the API reference exactly once.

The `-W` gate catches an ``automodule`` pointing at a module that no longer
exists. It cannot catch the opposite — a *new* module nobody added to the
reference — which is how docs/api.rst silently emptied itself during the
client/host split. This closes that gap.

Run it via ``make -C docs coverage``; CI runs the same target.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

DOCS = Path(__file__).resolve().parent
SRC = DOCS.parent / "src"
PACKAGE = "awsqueueengine"

AUTOMODULE = re.compile(r"^\s*\.\.\s+automodule::\s+([A-Za-z0-9_.]+)", re.MULTILINE)


def documented() -> list[str]:
    """Module names referenced by an ``automodule`` directive, with duplicates."""
    names: list[str] = []
    for rst in sorted(DOCS.rglob("*.rst")):
        # _build holds a copy of every source file under _sources/, which would
        # double-count every module and make the duplicate check useless.
        if "_build" in rst.parts:
            continue
        names.extend(AUTOMODULE.findall(rst.read_text()))
    return names


def actual() -> set[str]:
    """Importable modules in the package, excluding ``__init__`` files."""
    return {
        ".".join(path.relative_to(SRC).with_suffix("").parts)
        for path in (SRC / PACKAGE).rglob("*.py")
        if path.name != "__init__.py"
    }


def main() -> int:
    found = documented()
    missing = sorted(actual() - set(found))
    duplicated = sorted({name for name in found if found.count(name) > 1})

    for name in missing:
        print(f"::error::{name} is not in the API reference")
    for name in duplicated:
        print(f"::error::{name} is documented more than once")

    if missing or duplicated:
        print(f"\n{len(missing)} missing, {len(duplicated)} duplicated", file=sys.stderr)
        return 1

    print(f"all {len(actual())} modules documented exactly once")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
