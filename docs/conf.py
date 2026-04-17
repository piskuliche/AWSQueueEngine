import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"

sys.path.insert(0, str(SRC_ROOT))

project = "AWSQueueEngine"
author = "piskuliche"
copyright = "2026, piskuliche"
release = "0.1.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "alabaster"
html_static_path = ["_static"]
html_title = "AWSQueueEngine Documentation"

autodoc_mock_imports = ["mailtrap"]
autodoc_member_order = "bysource"
napoleon_google_docstring = True
napoleon_numpy_docstring = True
