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
import sys
from pathlib import Path

# Add ionbeam-client to Python path for autodoc
sys.path.insert(0, str(Path(__file__).parent.parent / "ionbeam-client"))

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
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]

# -- Autodoc configuration ---------------------------------------------------
autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "special-members": "__init__",
    "undoc-members": True,
    "exclude-members": "__weakref__",
}

autodoc_typehints = "description"
autodoc_typehints_description_target = "documented"

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_static_path = ["_static"]
release = "local-dev"