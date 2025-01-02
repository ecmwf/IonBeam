# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

import dataclasses
import json

# import codc as odc
import logging
import warnings
from collections import defaultdict
from dataclasses import field
from typing import Dict, Iterable, List, Literal

import pandas as pd
import pyodc as odc
from pyodc.codec import select_codec

from ..core.bases import Encoder, FileMessage, FinishMessage, TabularMessage
from ..core.config_parser.config_parser_machinery import ConfigError
from ..metadata import id_hash

warnings.simplefilter(action="error", category=FutureWarning)

logger = logging.getLogger(__name__)

# map ODB dtype enum to pandas type strings
dtype_lookup = {
    odc.DataType.INTEGER: "int64",
    odc.DataType.REAL: "float32",
    odc.DataType.STRING: "string",
    odc.DataType.DOUBLE: "float64",
}

# Functions to fill in the variable ODB fields


def source(msg: TabularMessage) -> pd.Series:
    "populates source@hdr"
    return msg.data.author

def decimal_encoded_month(msg: TabularMessage) -> pd.Series:
    dt = msg.data.time.dt
    return 100 * dt.year + dt.month


def decimal_encoded_date(msg: TabularMessage) -> pd.Series:
    "populates date@hdr and andate@desc"
    dt = msg.data.time.dt
    return 10000 * dt.year + 100 * dt.month + dt.day


def four_digit_hour(msg: TabularMessage) -> pd.Series:
    "populates time@hdr and antime@desc"
    # return msg.data.time.dt.hour.apply(lambda x: "{0:02d}00".format(x))
    return msg.data.time.dt.hour * 100

def start_time_four_digit(msg: TabularMessage) -> pd.Series:
    logger.debug("start time is runnning")
    return msg.data.start_time.dt.strftime('%H%M')

def minutes_with_fractional_seconds(msg: TabularMessage) -> pd.Series:
    "populates min@body"
    dt = msg.data.time.dt
    return dt.minute + dt.second / 60

def datetime(msg: TabularMessage) -> pd.Series:
    return msg.data.time.dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def altitude(msg: TabularMessage) -> pd.Series:
    "populates stalt@hdr"
    if "altitude" in msg.data.columns:
        return msg.data.altitude

    elif "air_pressure_near_surface" in msg.data.columns:
        p = msg.data.air_pressure_near_surface
        return 44330 * (1 - (p / 102300.15) ** (1 / 5.255))
    else:
        # raise ValueError(f"{msg} hsa neither pressure nor altitude data")
        return pd.Series([None for a in range(len(msg.data))])


def obsvalue(msg: TabularMessage) -> pd.Series:
    "function to populate obsvalue@body"
    return msg.data[msg.metadata.observation_variable]


def station_id(msg: TabularMessage) -> pd.Series:
    "function to populate  stationid@hdr with either station_id or track_id"
    if "station_id" in msg.data:
        strings = msg.data["station_id"]
    else:
        strings = msg.data["track_id"]

    return strings




def station_id_hash(msg: TabularMessage) -> pd.Series:
    if "station_id" in msg.data:
        strings = msg.data["station_id"]
    else:
        strings = msg.data["track_id"]

    return strings.apply(id_hash)


def dataset(msg: TabularMessage) -> pd.Series:
    if msg.metadata.source == "meteotracker":
        dataset = "track"
    else:
        dataset = msg.metadata.source
    dataset = msg.metadata.source

    return pd.Series([dataset for a in range(len(msg.data))])


ODC_Dtype = Literal["REAL", "DOUBLE", "INTEGER", "STRING"]


@dataclasses.dataclass
class MARS_Key:
    "Holds information about a MARS key and how to fill it in"
    name: str  # i.e codetype@desc
    dtype: ODC_Dtype

    # specify how this column will be filled
    fill_method: Literal["constant", "from_config", "from_metadata", "from_data", "function"]
    value: str | None = None
    key: str | None = None

    # metadata
    column_description: str | None = None

    by_observation_variable: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if self.fill_method == "from_config":
            pass
        elif self.fill_method == "constant":
            assert self.value is not None
        elif self.key is None:
            raise ValueError(f"{self.name} method: {self.fill_method} must have an associated key")

        self.dtype = getattr(odc.DataType, self.dtype)

    def values(self, msg: TabularMessage) -> pd.Series:
        "Bases on the information about and the message, return the values for this column"
        dtype = dtype_lookup[self.dtype]
        length = len(msg.data)
        match self.fill_method:
            case "constant":
                val = self.value
            case "from_config":
                obsvar = msg.metadata.observation_variable
                if obsvar not in self.by_observation_variable:
                    logger.warning(f"{self.name} doesn't have a value for {obsvar}. [{self.by_observation_variable}]")
                    return -1
                val = self.by_observation_variable[obsvar]
            case "from_metadata":
                val = getattr(msg.metadata, self.key)
            case "from_data":
                return msg.data[self.key].astype(dtype).reset_index(drop=True)
            case "function":
                if self.key not in globals():
                    raise ConfigError(f"Trying to fill the mars key '{self.name}' using function '{self.key}' but it isn't defined in the source code.")
                f = globals()[self.key]
                try:
                    out = f(msg).astype(dtype)
                except Exception as e:
                    raise ValueError(f"Error calling {f} with {msg} {f(msg) = }") from e
                return out.reset_index(drop=True)
            case _:
                raise ValueError

        with warnings.catch_warnings():
            warnings.simplefilter(action="ignore", category=FutureWarning)
            return pd.Series([val] * length, dtype=dtype)


@dataclasses.dataclass
class ODCEncoder(Encoder):
    """
    Encode data to ODB files.

    Args:
        output: A format string for the output path for each file.
        MARS_keys: Determines the output columns
    """
    output: str = "outputs/{source}/odb/{observation_variable}/{observation_variable}_{time_span.start}.odb"
    MARS_keys: List[MARS_Key] = dataclasses.field(default_factory=list)
    one_file_per_granule: bool = True
    split_data_columns: bool = False

    "Columns names to extract and put into the odb properties field"
    columns_to_metadata: list[str] = dataclasses.field(default_factory=list)

    seconds: bool = True
    minutes: bool = True

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)

        self.metadata = dataclasses.replace(self.metadata, state="odc_encoded")
        self.fdb_schema = globals.fdb_schema

        if not self.one_file_per_granule:
            self.output_file = self.resolve_path(self.output_file)
            self.output_file.parent.mkdir(exist_ok=True, parents=True)
            self.output_file.unlink(missing_ok=True)

        # Get all the MARS keys whose value is set by lookup in canonical variables
        # convert i.e obstype@desc to obstype in order to look it up in the canonical variables
        # mars_keys_to_annotate = {"obstype" : [obstype@hdr, obstype@desc], varno : ...}
        mars_keys_to_annotate = defaultdict(list)
        for key in self.MARS_keys:
            if key.fill_method == "from_config":
                if "@" in key.name:
                    mars_key, mars_type = key.name.split("@")
                else:
                    mars_key = key.name
                mars_keys_to_annotate[mars_key].append(key)

        # keys should be something like ["obstype", "codetype", "varno"]
        canonical_variables = {c.name: c for c in globals.canonical_variables}
        for var in canonical_variables.values():
            # canonical variables should have either no mars keys or all of them
            has_key = [getattr(var, m) is not None for m in mars_keys_to_annotate.keys()]
            if not any(has_key):
                continue
            if not all(has_key):
                raise ValueError(
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

    def create_output_df(self, msg: TabularMessage) -> pd.DataFrame:
        output_data = pd.DataFrame()
        for col in self.MARS_keys:

            # skip observed_value if we're not splitting data into multiple granules
            if not self.split_data_columns and col.name == "observed_value":
                continue

            try:
                output = col.values(msg)
                if len(output) != len(msg.data):
                    raise ValueError(f"Length of output {len(output)} != length of data {len(msg.data)}")
                
                output_data[col.name] = output
            except ValueError as e:
                raise e
            
            logger.debug(f"Added {col.name} to output data\n"
                         f"Fill Method: {col.fill_method}\n"
                         f"Data Length   : {len(output)}\n"
                         f"Length: {len(output_data[col.name])}\n"
                         f"# Unique Values: {output_data[col.name].nunique()}\n"
                         f"Values: {output_data[col.name]}")

        # Add in all the value columns
        if not self.split_data_columns:
            # logger.warning(f"{msg.data.columns = }")
            for name in msg.metadata.observation_variable.split(","):
                if name in msg.data and name not in output_data:
                    output_data[name] = msg.data[name]

        return output_data

    def encode(self, msg: TabularMessage | FinishMessage) -> Iterable[FileMessage]:
        if isinstance(msg, FinishMessage):
            return

        output_df = self.create_output_df(msg)
        logger.debug(f"Created output dataframe with columns {output_df.columns}")
        logger.debug(f"Output dataframe has {len(output_df)} rows")
        # logger.debug(f"{output_df = }")

        # Parse the odb file into a mars request
        # Grab the column name and value for every static column in the dataframe
        mars_request = {k: output_df[k].iloc[0] for k in output_df.columns if output_df[k].nunique() == 1}
        
        assert msg.metadata.time_span is not None
        logger.debug(f"{msg.metadata.time_span = }")
        mars_request["date"] = msg.metadata.time_span.start.strftime("%Y%m%d")
        mars_request["time"] = msg.metadata.time_span.start.strftime("%H%M")
        logger.debug(mars_request)

        logger.debug(f"Generated MARS keys for {msg}")
        mars_request, schema_branch = self.fdb_schema.parse(mars_request)

        # And encode the supplied data
        # logger.debug(f"Columns before encoding to ODC: {df.columns}")
        # logger.info(f"Encoded {msg} to {self.output}")
        additional_metadata = {
            "encoded_by": "IonBeam",  # TODO: add git commit hash or version here
            "IonBeam_git_hash": self.globals.code_source.git_hash,
        }

        # copy some data from the message metadata into the odb properties field
        if msg.metadata:
            for key in ["source", "observation_variable"]:
                val = getattr(msg.metadata, key)
                if val is not None:
                    additional_metadata[key] = val

            if msg.metadata.time_span is not None:
                additional_metadata["timeslice"] = str(msg.metadata.time_span.start.isoformat())

        # Copy additional data from the data itself into the properties
        for key in self.columns_to_metadata:
            assert msg.data[key].nunique() == 1
            additional_metadata[key] = msg.data[key].iloc[0]

        additional_metadata["mars_request"] = json.dumps(mars_request.as_strings())

        additional_metadata["columns"] = json.dumps(
            {
                key.name: {
                    "description": key.column_description,
                    # "unique_values": str(msg.data[key.name].unique()) if key.name in msg.data else None,
                }
                for key in self.MARS_keys
            }
        )

        if self.one_file_per_granule:
            kwargs = mars_request | dict(
                observation_variable=msg.metadata.observation_variable,
                source=msg.metadata.source,
                date=output_df["date"][0],
                external_id=output_df["external_id"][0],
                internal_id=output_df["internal_id"][0],
            )
            kwargs["start_time"] = msg.metadata.time_span.start.strftime("%H:%M:%S")
            
            f = self.output.format(**kwargs)

            self.output_file = self.globals.data_path / f
            self.output_file.parent.mkdir(parents=True, exist_ok=True)
            self.output_file.unlink(missing_ok=True)

        # Check if odc is going to be happy with the dtype conversions

        for col in self.MARS_keys:
            data = output_df[col.name]

            if pd.api.types.is_string_dtype(data.dtype) and data.hasnans:
                raise ValueError(f"column `{col.name}` has NaNs in a string column {data}")

            if dtype_lookup[col.dtype] != data.dtype:
                raise ValueError(
                    f"""{col.name}: pd.Series({data.dtype}) -> MARS_Key({repr(col.dtype)})
                    won't work!
                    {dtype_lookup[col.dtype] = }
                    {data.dtype = }
                    {col=}
                    {data=}
                    """
                )

            try:
                # Check that the target ODC dtype is compatible with the current representation
                # i.e we don't want to try to coerce strings to DataType.REAL

                codec = select_codec(col.name, data, col.dtype, None)
                # logger.debug(f"Chose codec {codec} for {col.name}, {data.dtype=}, {col.dtype=}")
            except AssertionError as e:
                raise ValueError(
                    f"""
Could not figure out ODC types for pd.Series({data.dtype}) -> {col}
{data.hasnans=}
{data.nunique()=}
Column of interest:
{data}

Whole output_df:
{output_df}
                """
                ) from e
            
        for col in output_df.columns:
            try:
                codec = select_codec(col, output_df[col], None, None)
            except RuntimeError as e:
                raise ValueError(
                    f"Failed to select dtype or codec for column {col}\n"
                    f"{output_df[col].dtype = }\n"
                    f"{output_df[col].nunique() = }\n"
                    f"{output_df[col].hasnans = }\n"
                    f"{output_df[col] = }\n"
                    f"{e}") from e

        with open(self.output_file, "wb" if self.one_file_per_granule else "ab") as fout:
            odc.encode_odb(
                output_df,
                fout,
                properties=additional_metadata,
                types={col.name: col.dtype for col in self.MARS_keys if col.dtype is not None},
            )

        output_msg = FileMessage(
            metadata=self.generate_metadata(
                msg, 
                time_span = msg.metadata.time_span,
                filepath=self.output_file,
                encoded_format="odb", mars_request=mars_request
            ),
        )
        yield self.tag_message(output_msg, msg)


encoder = ODCEncoder
