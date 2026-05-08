"""Sphinx configuration for IGEM documentation."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent

sys.path.insert(0, str(ROOT / "client" / "src"))
sys.path.insert(0, str(ROOT / "backend" / "src"))

project = "IGEM"
author = "Hall Lab"
copyright = f"{datetime.now().year}, Hall Lab"
release = "0.1.0"
version = "0.1"

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinx_design",
]

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "tasklist",
    "substitution",
    "linkify",
]

# Auto-generate anchors for h1–h3 headings so we can write
# [link](#heading-slug) in markdown without explicit labels.
myst_heading_anchors = 3

source_suffix = {
    ".md": "markdown",
    ".rst": "restructuredtext",
}

templates_path = ["_templates"]
exclude_patterns = [
    "_build",
    ".venv",
    "Thumbs.db",
    ".DS_Store",
    "**/.ipynb_checkpoints",
]

html_theme = "furo"
html_title = "IGEM Documentation"
html_static_path = ["_static"]
html_logo = "_static/logo.png"
html_css_files = ["custom.css"]

html_theme_options = {
    "announcement": (
        "<em>v0.1.0</em> — IGEM is in early access. "
        "<a href='/docs/release-notes.html'>See release notes</a>."
    ),
    "top_of_page_buttons": [],
    "light_css_variables": {
        "color-brand-primary": "#1e3a5f",
        "color-brand-content": "#2c8d8a",
        "color-announcement-background": "#eaf3f8",
        "color-announcement-text": "#1e3a5f",
        "color-sidebar-background": "#eaf3f8",
        "color-sidebar-background-border": "#dbe6ee",
        "color-sidebar-search-background": "#ffffff",
    },
    "dark_css_variables": {
        "color-brand-primary": "#6391c9",
        "color-brand-content": "#5cbab8",
        "color-announcement-background": "#101a30",
        "color-announcement-text": "#e6ecf2",
        "color-sidebar-background": "#101a30",
        "color-sidebar-background-border": "#1a2740",
        "color-sidebar-search-background": "#0a1020",
    },
}

autodoc_member_order = "bysource"
autodoc_typehints = "description"
autoclass_content = "both"

napoleon_google_docstring = True
napoleon_numpy_docstring = True

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
}
