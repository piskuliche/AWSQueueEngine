import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"

# autodoc imports the package for real, but it is deliberately NOT installed for
# a docs build: every third-party import in the package is lazy (boto3 and
# mailtrap are function-local, tomli is guarded behind Python < 3.11), so
# putting src/ on the path is enough. That also makes this build a tripwire —
# add a module-level `import boto3` and the docs go red. See contributing.rst.
sys.path.insert(0, str(SRC_ROOT))


def _version_from_setup_py() -> str:
    """Single-source the version from setup.py so the two cannot drift."""
    text = (PROJECT_ROOT / "setup.py").read_text()
    match = re.search(r"version\s*=\s*['\"]([^'\"]+)['\"]", text)
    return match.group(1) if match else "0.0.0"


project = "AWSQueueEngine"
author = "piskuliche"
copyright = "2026, piskuliche"
release = _version_from_setup_py()
version = release

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "furo"
html_static_path = ["_static"]
html_title = "AWSQueueEngine Documentation"

html_theme_options = {
    "source_repository": "https://github.com/piskuliche/AWSQueueEngine/",
    "source_branch": "main",
    "source_directory": "docs/",
}

autodoc_default_options = {
    "members": True,
    "show-inheritance": True,
    # Never render a constant's runtime value into the HTML. host/config.py
    # reads AWSQUEUEENGINE_MAILTRAP_TOKEN at import time, so a local build with
    # the token exported would otherwise bake the secret into the page, and
    # shared/paths.py would publish the build machine's home directory.
    "no-value": True,
}
autodoc_member_order = "bysource"
# Render a default argument as the source text that produced it, not its repr.
# Without this, `def f(path=MONITOR_STATE_FILE)` publishes
# PosixPath('/home/<builder>/.awsqe/...') — the build machine's home directory
# baked into the signature. Reads better, too.
autodoc_preserve_defaults = True
napoleon_google_docstring = True
napoleon_numpy_docstring = True
