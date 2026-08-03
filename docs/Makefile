SPHINXBUILD ?= sphinx-build
SPHINXOPTS  ?=
SOURCEDIR    = .
BUILDDIR     = _build

.PHONY: help html linkcheck clean

help:
	@echo "html       build the HTML docs into $(BUILDDIR)/html"
	@echo "linkcheck  check external links (advisory; not part of the CI gate)"
	@echo "clean      remove $(BUILDDIR)"
	@echo
	@echo "Override the build flags with e.g. SPHINXOPTS=  to stop treating"
	@echo "warnings as errors, or SPHINXOPTS='-W --exception-on-warning -T'"
	@echo "to get a traceback at the warning site."

html:
	$(SPHINXBUILD) -b html $(SPHINXOPTS) $(SOURCEDIR) $(BUILDDIR)/html

linkcheck:
	$(SPHINXBUILD) -b linkcheck $(SOURCEDIR) $(BUILDDIR)/linkcheck

clean:
	rm -rf $(BUILDDIR)
