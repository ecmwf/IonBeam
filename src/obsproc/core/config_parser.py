from dataclasses import dataclass, field
from typing import List, Literal
import logging
from pathlib import Path

import yaml
from yamlinclude import YamlIncludeConstructor


from dataclass_wizard import JSONWizard, LoadMixin
from dataclass_wizard.errors import UnknownJSONKey, MissingFields

from collections import defaultdict

# Perhaps this could be automated if the list gets longer
from ..sources.multi_file import MultiFileSource
from ..sources.cima import CIMASource
from ..sources.meteotracker import MeteoTrackerSource, MeteoTrackerOfflineSource
from ..sources.watch_directory import WatchDirectorySource

from ..parsers import CSVParser

from ..aggregators.by_time import TimeAggregator

from ..encoders.csv import CSVEncoder
from ..encoders.odb import ODCEncoder
from ..encoders.dummy import DummyEncoder

from .converters import unit_conversions

from .bases import CanonicalVariable

# from ..sources.meteotracker import MeteoTrackerSource

# These types have to be defined as unions rather using the fact the all inherit
# from the same base class because the config parse expects unions of concrete classes
# Python does have a __subclasses__ attribute so in principle one could make this work
Source = CIMASource | MultiFileSource | WatchDirectorySource | MeteoTrackerSource | MeteoTrackerOfflineSource
Parser = CSVParser
Aggregator = TimeAggregator
Encoder = ODCEncoder | CSVEncoder | DummyEncoder


Stage = Literal["sources", "preprocessors", "aggregators", "encoders"]

logger = logging.getLogger(__name__)


@dataclass
class Config(JSONWizard, LoadMixin):
    canonical_variables: List[CanonicalVariable]
    pipeline: List[str]
    sources: List[Source] = field(default_factory=list)
    parsers: List[Parser] = field(default_factory=list)
    aggregators: List[Aggregator] = field(default_factory=list)
    encoders: List[Encoder] = field(default_factory=list)

    class _(JSONWizard.Meta):
        debug_enabled = False

        # Be strict with parsing the YAML, if you provide a key which is not in the
        # correspinding dataclass then it will throw and error
        raise_on_unknown_json_key = True

        # Tell datacalss_wizard that when we try to load in a field which is a union of types
        # like Parse = CSVParser | XLSXParser
        # then use the name field to figure out which it's meant to be
        # This means CSVParser fields in the config must contain a name: CSVParser entry
        tag_key = "name"

        # Use the class name i.e CSVParser as the key in the config file
        # could override this but seems like the simplest choice
        auto_assign_tags = True


class ConfigError(BaseException):
    pass


def parse_config(yaml_file: Path):
    try:
        # Set up the yaml parser to support file includes
        YamlIncludeConstructor.add_to_loader_class(loader_class=yaml.FullLoader, base_dir=str(yaml_file.parent))

        # Load the yaml file
        with open(yaml_file) as f:
            data = yaml.load(f, Loader=yaml.FullLoader)

        # Parse the yaml file using python dataclasses as a template
        config = Config.from_dict(data)

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
                    and f"{col.unit.strip()} -> {col.canonical_variable.unit.strip()}" not in unit_conversions
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
                    has_key = [getattr(var, m) is not None for m in mars_keys_to_annotate.keys()]
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
                            mars_key.by_observation_variable[var.name] = getattr(var, mars_name)

    except UnknownJSONKey as e:
        raise ConfigError(
            f"""
        Config decode error: {type(e).__name__}
        In section: {e.class_name}
        Unknown field: {e.json_key}
        Allowed fields: {e.fields}"""
        )
    except MissingFields as e:
        raise ConfigError(
            f"""Config decode error: {type(e).__name__}
            In section: {e.class_name}
            Missing fields: {e.missing_fields}
            Provided fields: {e.fields}
            Original Exception: {e}"""
        )
    # except ParseError as e:
    #     issues = "\n".join(f"{k} : {v}" for k, v in e.kwargs.items())
    #     raise ConfigError(
    #         f"""Config decode error:
    #         In section: {e.class_name}
    #         In field: {e.field_name}
    #         Incorrect value: {str(e.obj)[:100]}
    #         {issues}"""
    #     )
    else:
        return config
