# Configuration file for the Sphinx documentation builder.
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# NOTE: This configuration uses autoapi without automatic generation and
# instead uses directives (see api.rst). This is done because with just
# autoapi auto generation all modules must be imported. In ource case this
# would mean we need to complie our python bindings. Using autoapi with
# directives allows to use 'static analysis' mode to extract docstrings.default_mod
# See: https://sphinx-autoapi.readthedocs.io/en/stable/how_to.html#how-to-transition-to-autodoc-style-documentation
# See: https://sphinx-autoapi.readthedocs.io/en/stable/reference/directives.html

import datetime

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information


project = "Ionbeam"
copyright = f"{datetime.datetime.today().year}, ECMWF"
author = "ECMWF"
version = "local-dev"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinxcontrib.mermaid",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_static_path = ["_static"]
release = "local-dev"
