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
from .bases import Source, Parser, Aggregator, Encoder, CanonicalVariable
from .config_parser_machinery import parse_config_from_dict, ConfigError

# This line is necessary to automatically find all the subclasses of things like "Encoder"
from .. import sources, parsers, aggregators, encoders

logger = logging.getLogger()


# Set up the yaml parser to include line numbers with the key "__line__"
class SafeLineLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        mapping = super(SafeLineLoader, self).construct_mapping(node, deep=deep)
        # Add 1 so line numbering starts at 1
        mapping["__line__"] = node.start_mark.line + 1
        return mapping


# The overall config file spec
@dataclass
class Config:
    canonical_variables: List[CanonicalVariable]
    pipeline: List[str]
    sources: List[Source] = field(default_factory=list)
    parsers: List[Parser] = field(default_factory=list)
    aggregators: List[Aggregator] = field(default_factory=list)
    encoders: List[Encoder] = field(default_factory=list)


def parse_config(yaml_file: Path):
    # Set up the yaml parser to support file includes
    YamlIncludeConstructor.add_to_loader_class(
        loader_class=SafeLineLoader, base_dir=str(yaml_file.parent)
    )

    # Load the yaml file
    with open(yaml_file) as f:
        config_dict = yaml.load(f, Loader=SafeLineLoader)

    # Parse the yaml file using python dataclasses as a template
    config = parse_config_from_dict(Config, config_dict, filepath=yaml_file)

    # Do post load work that links data from one place to another
    # This is mostly giving information about the canonical variables to other
    # parts of the system
    canonical_variables = {c.name: c for c in config.canonical_variables}
    for p in config.parsers:
        for col in p.all_columns:
            # Check that preprocessors only create columns with canonical names
            if col.name not in canonical_variables:
                raise ConfigError(f"{col.name} not in canonical names!")

            # Just copy the whole canonical variable object onto it
            col.canonical_variable = canonical_variables[col.name]

            # Emit a warning if the config doesn't include the unit conversion
            if (
                col.unit != col.canonical_variable.unit
                and f"{col.unit.strip()} -> {col.canonical_variable.unit.strip()}"
                not in unit_conversions
            ):
                logger.warning(
                    f"No unit conversion registered for {col.name} {col.unit} -> {col.canonical_variable.unit}|"
                )

            # Tell the parsers what dtypes they need to use
            col.dtype = canonical_variables[col.name].dtype

    for encoder in config.encoders:
        if isinstance(encoder, ODCEncoder):
            # Get all the MARS keys whose value is set by lookup in canonical variables
            # convert i.e obstype@desc to obstype in order to look it up in the canonical variables
            # mars_keys_to_annotate = {"obstype" : [obstype@hdr, obstype@desc], varno : ...}
            mars_keys_to_annotate = defaultdict(list)
            for key in encoder.MARS_keys:
                if key.fill_method == "from_config":
                    mars_name, mars_type = key.name.split("@")
                    mars_keys_to_annotate[mars_name].append(key)

            # keys should be something like ["obstype", "codetype", "varno"]

            for var in canonical_variables.values():
                # canonical variables should have either no mars keys or all of them
                has_key = [
                    getattr(var, m) is not None for m in mars_keys_to_annotate.keys()
                ]
                if not any(has_key):
                    continue
                if not all(has_key):
                    raise ConfigError(
                        f"{var.name} has only a partial set of the necessary \
                            MARS keys {mars_keys_to_annotate.keys()}"
                    )

                # Copy the value for each mars key over so a canonical variable with
                # air_temp
                #    varno: 5
                # will lead to the mars key object for varno having
                #  key.name == "varno@body"
                #  key.by_observation_variable["air_temp"] == 5
                for mars_name, mars_keys in mars_keys_to_annotate.items():
                    for mars_key in mars_keys:
                        mars_key.by_observation_variable[var.name] = getattr(
                            var, mars_name
                        )
    return config
