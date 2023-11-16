# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

import logging

from dataclasses import dataclass, field
from typing import List, Literal
from pathlib import Path
from collections import defaultdict

# yamlinclude allows us to include one yaml file from another
import yaml
from yamlinclude import YamlIncludeConstructor

from .bases import CanonicalVariable, Action, Globals
from .history import describe_code_source, CodeSourceInfo
from .config_parser_machinery import parse_config_from_dict, ConfigError

# # This line is necessary to automatically find all the subclasses of things like "Encoder"
from .. import sources, parsers, aggregators, quality_assessment, encoders, writers

logger = logging.getLogger(__name__)


# Set up the yaml parser to include line numbers with the key "__line__"
class SafeLineLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        mapping = super(SafeLineLoader, self).construct_mapping(node, deep=deep)
        # Add 1 so line numbering starts at 1
        mapping["__line__"] = node.start_mark.line + 1
        return mapping


# The overall config file spec
@dataclass
class SubConfig:
    actions: List[Action] = field(default_factory=list)


# The overall config file spec
@dataclass
class Config:
    globals: Globals
    sources: List[str]


def parse_sub_config(yaml_file: Path, globals, schema=SubConfig):
    """
    Refering to the docs for parse_config, this function deals with the subfolders:
    ├── acronet - a set of folders for each logically distinct source of data.
    ├── MARS_keys.yaml - Info on how to create a MARS key for this source
    └── actions.yaml - the actions that define the pipeline for this source

    It loads in the actions, MARS_keys.yaml is include'd with a directive.
    It then calls action.init() to give all the actions a change to look at the global config.
    """
    # Set up the yaml parser to support file includes
    YamlIncludeConstructor.add_to_loader_class(loader_class=SafeLineLoader, base_dir=str(yaml_file.parent))

    # Load the yaml file
    with open(yaml_file) as f:
        config_dict = yaml.load(f, Loader=SafeLineLoader)

    # Parse the yaml file using python dataclasses as a template
    config = parse_config_from_dict(schema, config_dict, filepath=yaml_file)

    # Let all the actions initialise themselves with access to the global config
    for action in config.actions:
        action.init(globals)

    return config


def parse_config(config_dir: Path, schema=Config):
    """
    Config directory has the structure:
    .
    ├── config.yaml - Top level config
    ├── canonical_variables.yaml - A list of all known variables with metadata
    ├── secrets.yaml - not checked into source control
    ├── acronet - a set of folders for each logically distinct source of data.
    │   ├── MARS_keys.yaml - Info on how to create a MARS key for this source
    │   └── actions.yaml - the actions that define the pipeline for this source
    ├── meteotracker
    │   ├── MARS_keys.yaml
    │   └── actions.yaml
    └── sensor.community
        ├── MARS_keys.yaml
        └── actions.yaml
    """
    YamlIncludeConstructor.add_to_loader_class(loader_class=SafeLineLoader, base_dir=str(config_dir))

    if not config_dir.exists():
        raise ConfigError(f"{config_dir} does not exist!")

    global_config_file = config_dir / "config.yaml"
    if not global_config_file.exists():
        raise ConfigError(f"Could not find config.yaml in {config_dir}")

    with open(global_config_file) as f:
        data = yaml.load(f, Loader=SafeLineLoader)
        global_config = parse_config_from_dict(Config, data, filepath=global_config_file)

    logger.debug(f"Loaded global config...")

    # Resolve the paths in the global config relative to the config file
    for name in ["data_path", "config_path"]:
        path = getattr(global_config.globals, name)
        if not path.is_absolute():
            path = (config_dir.parent / path).resolve()
        setattr(global_config.globals, name, path)
        logger.debug(f"Resolved global_config.globals.{name} to {path}")

    # # Figure out the git status clean/dirty and the current git hash
    global_config.globals.code_source = describe_code_source()
    logger.debug(f"Checked repository source state: {global_config.globals.code_source}")

    # Loop over one level of subdirectories and read in the actions from each one
    action_source_files = {
        source_name: config_dir / source_name / "actions.yaml" for source_name in global_config.sources
    }
    action_source_files["base"] = config_dir / "actions.yaml"

    sources = {}
    actions = []
    for name, file in action_source_files.items():
        assert file.exists()
        logger.debug(f"Parsing {file}")
        source = parse_sub_config(file, globals=global_config.globals, schema=SubConfig)
        sources[name] = source
        actions.extend(source.actions)

    global_config.sources = sources
    return global_config, actions
