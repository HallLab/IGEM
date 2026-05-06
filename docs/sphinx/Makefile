SOURCEDIR := .
HTMLDIR   := ../../frontend/public/docs
VENV      := .venv
SPHINX    := $(VENV)/bin/sphinx-build
AUTO      := $(VENV)/bin/sphinx-autobuild
PYTHON    := python3

.PHONY: help install html watch clean

help:
	@echo "Targets:"
	@echo "  install  Create $(VENV) and install requirements (run once)"
	@echo "  html     Build HTML docs into $(HTMLDIR)"
	@echo "  watch    Live-reload preview at http://localhost:8000"
	@echo "  clean    Remove the built HTML directory"

install:
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -r requirements.txt

html:
	@test -x $(SPHINX) || { echo "Sphinx not found. Run 'make install' first."; exit 1; }
	$(SPHINX) -b html $(SOURCEDIR) $(HTMLDIR)

watch:
	@test -x $(AUTO) || { echo "sphinx-autobuild not found. Run 'make install' first."; exit 1; }
	$(AUTO) $(SOURCEDIR) $(HTMLDIR) --port 8000

clean:
	rm -rf $(HTMLDIR)
