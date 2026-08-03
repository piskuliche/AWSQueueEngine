Contributing
============

Running the tests
-----------------

.. code-block:: bash

   python -m pytest

Building the docs
-----------------

.. code-block:: bash

   python -m pip install -r docs/requirements.txt
   make -C docs clean html
   python -m http.server -d docs/_build/html 8000

Always ``clean`` first when checking for warnings: incremental builds skip
unchanged files and will happily hide one.

Warnings are errors
~~~~~~~~~~~~~~~~~~~

``docs/Makefile`` sets ``SPHINXOPTS ?= -W``, and CI runs the same
``make -C docs html``, so a warning fails the build. To iterate without that:

.. code-block:: bash

   make -C docs html SPHINXOPTS=                              # warnings stay warnings
   make -C docs html SPHINXOPTS="-W --exception-on-warning -T" # traceback at the warning

The most common way to trip it is adding a page and forgetting to list it in a
toctree in ``docs/index.rst`` — Sphinx reports that as
``document isn't included in any toctree``. Add the page and the toctree entry
in the same commit.

Every module must be documented
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   make -C docs coverage

``-W`` catches an ``automodule`` pointing at a module that no longer exists. It
cannot catch the opposite — a *new* module nobody added to the reference — and
that is half of how ``docs/api.rst`` silently emptied itself during the
client/host split. This check closes that gap, so a new module means a new
entry under ``docs/api/``.

Why the package is not installed for a docs build
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``docs/conf.py`` puts ``src/`` on ``sys.path``, and every third-party import in
the package is lazy — ``boto3`` and ``mailtrap`` are function-local, ``tomli``
is guarded behind Python < 3.11. So Sphinx alone is enough to import all of it.

That is deliberate, not an oversight: it makes the docs build a tripwire for the
lazy-import discipline. Add a module-level ``import boto3`` and the docs go red.
If you genuinely need a module-level third-party import, that is a decision to
make explicitly, not to discover later.

.. note::

   Building on Python 3.10 additionally needs ``tomli``. CI builds on 3.12.

Publishing
----------

``.github/workflows/docs.yml`` builds on every pull request, uploading the HTML
as an artifact you can download to preview, and deploys to GitHub Pages on push
to ``main``.

Conventions
-----------

Branches are named ``<issue-number>-<type>-<slug>``, e.g.
``25-feat-docs-github-pages``. Commit messages follow Conventional Commits with
an optional scope: ``feat(client):``, ``fix(host):``, ``docs:``, ``ci:``.

Docs are the canonical reference. When behaviour changes, update the relevant
page here rather than only the README.
