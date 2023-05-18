import os
import sys
from importlib import import_module

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points

REGISTERED_PLUGINS = {}


def register_plugin(category, name, klass):
    category_lookup = REGISTERED_PLUGINS.setdefault(category, dict())
    category_lookup[name] = klass


def find_plugin(search_dir, search_module, category, plugin_name):
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

    for filename in os.listdir(search_dir):
        name, ext = os.path.splitext(filename)
        if name[0] != "_" and ext == ".py":
            if name == name_lower:
                mod = import_module(f".{name}", package=search_module)
                klass = getattr(mod, category)
                category_lookup[plugin_name] = klass
                return klass
            candidates.add(name)

    # Look for other plugins

    for e in entry_points(group=f"obsproc.{category}"):
        if e.name == plugin_name or e.name == name_lower:
            klass = e.load()
            category_lookup[plugin_name] = klass
            return klass
        candidates.add(e.name)

    candidates = {c.replace("_", "-") for c in candidates}
    raise NameError(f"Cannot find {category} plugin '{plugin_name}'. Candidates are: {candidates}")
