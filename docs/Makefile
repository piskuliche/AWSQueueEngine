SPHINXBUILD ?= sphinx-build
# Warnings are errors. A renamed or deleted module silently emptied the API
# reference once already; this is what stops it happening again.
SPHINXOPTS  ?= -W
SOURCEDIR    = .
BUILDDIR     = _build

.PHONY: help html coverage linkcheck clean

help:
	@echo "html       build the HTML docs into $(BUILDDIR)/html"
	@echo "coverage   assert every module is in the API reference exactly once"
	@echo "linkcheck  check external links (advisory; not part of the CI gate)"
	@echo "clean      remove $(BUILDDIR)"
	@echo
	@echo "Override the build flags with e.g. SPHINXOPTS=  to stop treating"
	@echo "warnings as errors, or SPHINXOPTS='-W --exception-on-warning -T'"
	@echo "to get a traceback at the warning site."

html:
	$(SPHINXBUILD) -b html $(SPHINXOPTS) $(SOURCEDIR) $(BUILDDIR)/html

coverage:
	python3 $(SOURCEDIR)/check_module_coverage.py

linkcheck:
	$(SPHINXBUILD) -b linkcheck $(SOURCEDIR) $(BUILDDIR)/linkcheck

clean:
	rm -rf $(BUILDDIR)
