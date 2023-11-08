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

from .converters import unit_conversions
from ..encoders.odb import ODCEncoder
from .bases import Source, Processor, Parser, Aggregator, Encoder, CanonicalVariable
from .history import describe_code_source, CodeSourceInfo
from .config_parser_machinery import parse_config_from_dict, ConfigError

# This line is necessary to automatically find all the subclasses of things like "Encoder"
from .. import sources, parsers, aggregators, quality_assessment, encoders

logger = logging.getLogger(__name__)


# Set up the yaml parser to include line numbers with the key "__line__"
class SafeLineLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        mapping = super(SafeLineLoader, self).construct_mapping(node, deep=deep)
        # Add 1 so line numbering starts at 1
        mapping["__line__"] = node.start_mark.line + 1
        return mapping


@dataclass
class GlobalConfig:
    canonical_variables: List[CanonicalVariable]
    config_path: Path
    data_path: Path
    code_source: CodeSourceInfo | None = None


# The overall config file spec
@dataclass
class Config:
    global_config: GlobalConfig
    pipeline: List[str]
    sources: List[Source] = field(default_factory=list)
    parsers: List[Parser] = field(default_factory=list)
    aggregators: List[Aggregator] = field(default_factory=list)
    encoders: List[Encoder] = field(default_factory=list)
    other_processors: List[Processor] = field(default_factory=list)


def parse_config(yaml_file: Path, schema=Config):
    # Set up the yaml parser to support file includes
    YamlIncludeConstructor.add_to_loader_class(loader_class=SafeLineLoader, base_dir=str(yaml_file.parent))

    # Load the yaml file
    with open(yaml_file) as f:
        config_dict = yaml.load(f, Loader=SafeLineLoader)

    # Parse the yaml file using python dataclasses as a template
    config = parse_config_from_dict(schema, config_dict, filepath=yaml_file)

    # Resolve the paths in the global config relative to the config file
    for name in ["data_path", "config_path"]:
        path = getattr(config.global_config, name)
        if not path.is_absolute():
            path = (yaml_file.parent / path).resolve()
        setattr(config.global_config, name, path)
        logger.debug(f"Resolved global_config.{name} to {path}")

    # Figure out the git status clean/dirty and the current git hash
    config.global_config.code_source = describe_code_source()

    # Let all the actions initialise themselves with access to the global config
    for stage in config.pipeline:
        for action in getattr(config, stage):
            action.init(config.global_config)

    return config
