# Configuration file for the Sphinx documentation builder.
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import datetime
import sys
from pathlib import Path
from typing import Dict, List

# Add monorepo root to Python path for version import
sys.path.insert(0, str(Path(__file__).parent.parent))
# Add ionbeam-client to Python path for autodoc
sys.path.insert(0, str(Path(__file__).parent.parent / "ionbeam-client"))

# Import shared version
try:
    from _version import __version__
    version = __version__
    release = __version__
except ImportError:
    version = "local-dev"
    release = "local-dev"

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information


project = "Ionbeam"
copyright = f"{datetime.datetime.today().year}, ECMWF"
author = "ECMWF"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinxcontrib.mermaid",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosectionlabel",
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

html_theme = "pydata_sphinx_theme"
# Do not show source link on each page
html_sidebars: Dict[str, List[str]] = {
    "**": [],
}

html_theme_options = {
    "navbar_align": "left",
    "navbar_start": ["navbar-logo"],
    "navbar_center": ["navbar-nav"],
    "navbar_end": ["navbar-icon-links", "theme-switcher", "version-switcher"],
    "navbar_persistent": ["search-button"],
    "primary_sidebar_end": [],
    # On local builds no version.json is present
    "check_switcher": False
}

html_context = {
    # Enable auto detection of light/dark mode
   "default_mode": "auto"
}

# -- autosectionlabel configuration ------------------------------------------
autosectionlabel_prefix_document = True