# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

import sys
from pathlib import Path
from importlib import import_module

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points

REGISTERED_PLUGINS = {}


def register_plugin(category, name, klass):
    category_lookup = REGISTERED_PLUGINS.setdefault(category, dict())
    category_lookup[name] = klass


def find_plugin(search_dir: Path, search_module: str, category: str, plugin_name: str):
    """
    Plugins can be found in three places

    1. A class belonging to the relevant category can have been explicitly registered by a call to register_plugin

    2. A class can be created in the plugin module's directory, for standard 'plugins'

    3. An entrypoint can be registered by an external package, in its setup.py

        setuptools.setup(
            entry_points={'obsproc.sources': [
                'source-name = package_name:ClassName'
            ]},
        )
    """

    candidates = set()
    name_lower = plugin_name.replace("-", "_")

    # Lookup if this plugin has been called before

    category_lookup = REGISTERED_PLUGINS.setdefault(category, dict())
    for n in (plugin_name, name_lower):
        if n in category_lookup:
            return category_lookup[n]

    candidates.update(category_lookup.keys())

    # First look in the local directories
    for filename in search_dir.iterdir():
        if filename.stem[0] != "_":
            if filename.stem == name_lower:
                mod = import_module(f".{filename.stem}", package=search_module)
                klass = getattr(mod, category)
                category_lookup[plugin_name] = klass
                return klass
            candidates.add(filename.stem)

    # Look for other plugins

    for e in entry_points(group=f"obsproc.{category}"):
        if e.name == plugin_name or e.name == name_lower:
            klass = e.load()
            category_lookup[plugin_name] = klass
            return klass
        candidates.add(e.name)

    candidates = {c.replace("_", "-") for c in candidates}
    raise NameError(f"Cannot find {category} plugin '{plugin_name}'. Candidates are: {candidates}")
