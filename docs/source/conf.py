# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "IGEM"
copyright = "2023, Hall Lab"
author = "Hall Lab"
release = "0.1.2"
version = "0.1.2"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    'rst2pdf.pdfbuilder',
]

templates_path = ["_templates"]
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# html_theme = 'alabaster'
# html_theme = "cloud"
html_theme = "sphinx_rtd_theme"

html_static_path = ["_static"]

html_logo = "_static/pictures/logo.jpg"


# Set the master file name to the root .rst file of your documentation
master_doc = 'index'

# Add LaTeX options
latex_elements = {
    'papersize': 'letterpaper',
    'pointsize': '10pt',
}

pdf_documents = [('index', u'rst2pdf', u'Sample rst2pdf doc', u'Your Name'),]