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
from typing import Literal, List, Dict, Iterable
from pathlib import Path

from ..core.bases import TabularMessage, FinishMessage, FileMessage, Encoder
from ..core.fdb_schema_parser import FDBSchema

import pandas as pd

import hashlib

from dataclasses import field
from collections import defaultdict

# try:
#     import codc as odc
# except ImportError:
#     import pyodc as odc

import pyodc as odc

import logging

import warnings

warnings.simplefilter(action="error", category=FutureWarning)

logger = logging.getLogger(__name__)

# map ODB dtype enum to pandas type strings
dtype_lookup = {
    odc.DataType.INTEGER: "int64",
    odc.DataType.REAL: "float32",
    odc.DataType.STRING: "object",
    odc.DataType.DOUBLE: "float64",
}

# Functions to fill in the variable ODB fields


def source(msg: TabularMessage) -> pd.Series:
    "populates source@hdr"
    return msg.data.author
    # return pd.Series([msg.metadata.source for _ in range(len(msg.data))])

    mapping = {
        "genova": "GENOALL",
        "jerusalem": "TAULL",
        "llwa": "LLWA",
        "barcelona": "UBLL",
        "hasselt": "UHASSELT",
        None: "IOT",
    }

    def author_to_source(author):
        shortname = author.split("_")[0] if "_living_lab" in author else None
        return mapping[shortname]

    return pd.Series([author_to_source(a) for a in msg.data.author.values])


def decimal_encoded_date(msg: TabularMessage) -> pd.Series:
    "populates date@hdr and andate@desc"
    dt = msg.data.time.dt
    return 10000 * dt.year + 100 * dt.month + dt.day


def four_digit_hour(msg: TabularMessage) -> pd.Series:
    "populates time@hdr and antime@desc"
    # return msg.data.time.dt.hour.apply(lambda x: "{0:02d}00".format(x))
    return msg.data.time.dt.hour * 100


def minutes_with_fractional_seconds(msg: TabularMessage) -> pd.Series:
    "populates min@body"
    dt = msg.data.time.dt
    return dt.minute + dt.second / 60


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

    def f(string):
        return hashlib.sha256(str(string).encode()).hexdigest()[:8]

    return strings
    return strings.apply(f)


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
                val = getattr(msg.metadata, self.key, None) or msg.metadata[self.key]
            case "from_data":
                return msg.data[self.key].astype(dtype)
            case "function":
                f = globals()[self.key]
                return f(msg).astype(dtype)
            case _:
                raise ValueError

        with warnings.catch_warnings():
            warnings.simplefilter(action="ignore", category=FutureWarning)
            return pd.Series([val] * length, dtype=dtype)


@dataclasses.dataclass
class ODCEncoder(Encoder):
    output: str
    MARS_keys: List[MARS_Key]
    fdb_schema: FDBSchema | None = None
    one_file_per_granule: bool = True
    seconds: bool = True
    minutes: bool = True

    def __str__(self):
        return f"{self.__class__.__name__}({self.match})"

    def init(self, globals):
        super().init(globals)

        self.metadata = dataclasses.replace(self.metadata, state="odc_encoded")

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
                mars_name, mars_type = key.name.split("@")
                mars_keys_to_annotate[mars_name].append(key)

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
        output_data = {}
        for col in self.MARS_keys:
            try:
                output_data[col.name] = col.values(msg)
            except ValueError as e:
                print(col)
                raise e
        return pd.DataFrame(output_data)

    def encode(self, msg: TabularMessage | FinishMessage) -> Iterable[FileMessage]:
        if isinstance(msg, FinishMessage):
            return

        obsval_dtype = msg.data[msg.metadata.observation_variable].dtype
        if str(obsval_dtype).startswith("str") or str(obsval_dtype) == "object":
            logger.warning(
                f"Can't output {msg.metadata.observation_variable} \
                            as observation variable because it has dtype {obsval_dtype}"
            )
            return

        output_df = self.create_output_df(msg)

        if self.fdb_schema is not None:
            # Remove everything past the @ in the odb column names
            kwords = {k.split("@")[0]: output_df[k].iloc[0] for k in output_df.columns}

            # Check the column names and first value against the schema
            logger.debug(f"Generated MARS keys for {msg}")
            key_matches = list(self.fdb_schema.match(kwords))

            for k in key_matches:
                logger.debug(k.info())

            if not all(k.good() for k in key_matches):
                raise ValueError("Generated ODC file does not match given schema!")

        # And encode the supplied data
        # logger.debug(f"Columns before encoding to ODC: {df.columns}")
        # logger.info(f"Encoded {msg} to {self.output}")
        additional_metadata = {
            "encoded_by": "IonBeam",  # TODO: add git commit hash or version here
            "IonBeam_git_hash": self.globals.code_source.git_hash,
        }
        if msg.metadata:
            for key in ["source", "observation_variable"]:
                val = getattr(msg.metadata, key)
                if val is not None:
                    additional_metadata[key] = val

            if msg.metadata.time_slice is not None:
                additional_metadata["timeslice"] = str(msg.metadata.time_slice.start_time.isoformat())

        if self.one_file_per_granule:
            f = "outputs/{source}/odb/{observation_variable}/{date}_{time:04d}.odb"
            kwargs = dict(
                observation_variable=msg.metadata.observation_variable,
                source=msg.metadata.source,
                date=output_df["date@hdr"][0],
                time=output_df["time@hdr"][0],
            )
            f = f.format(**kwargs)

            self.output_file = self.globals.data_path / f
            self.output_file.parent.mkdir(parents=True, exist_ok=True)
            self.output_file.unlink(missing_ok=True)

        # Check if odc is going to be happy with the dtype conversions
        from pyodc.codec import select_codec

        for col in self.MARS_keys:
            data = output_df[col.name]

            if dtype_lookup[col.dtype] != data.dtype:
                raise ValueError(
                    f"""{col.name}: pd.Series({data.dtype}) -> MARS_Key({repr(col.dtype)})
                    won't work!
                    {col=}
                    {data=}
                    """
                )

            try:
                # Check that the target ODC dtype is compatible with the current representation
                # i.e we don't want to try to coerce strings to DataType.REAL

                select_codec(col.name, data, col.dtype, None)
                # logger.debug(f"Chose codec {codec} for {col.name}, {data.dtype=}, {col.dtype=}")
            except AssertionError:
                raise ValueError(
                    f"""
                Could not figure out ODC types for pd.Series({data.dtype}) -> {col}
                {data.hasnans=}
                {data.nunique()=}
                {data}
                """
                )

        with open(self.output_file, "wb" if self.one_file_per_granule else "ab") as fout:
            odc.encode_odb(
                output_df,
                fout,
                properties=additional_metadata,
                types={col.name: col.dtype for col in self.MARS_keys if col.dtype is not None},
            )

        output_msg = FileMessage(
            metadata=self.generate_metadata(msg, filepath=self.output_file, encoded_format="odb"),
        )
        yield self.tag_message(output_msg, msg)


encoder = ODCEncoder
