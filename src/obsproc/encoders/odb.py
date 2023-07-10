import dataclasses
from typing import Literal, List, Dict
from pathlib import Path

from ..core.bases import TabularMessage, FinishMessage, FileMessage
from .bases import Encoder

import pandas as pd

import hashlib

from dataclasses import field

# try:
#     import codc as odc
# except ImportError:
#     import pyodc as odc

import pyodc as odc
from dataclasses import replace

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
        raise ValueError(f"{msg} hsa neither pressure nor altitude data")


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
        return hashlib.sha256(string.encode()).hexdigest()[:8]

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
    one_file_per_granule: bool = True
    seconds: bool = True
    minutes: bool = True
    name: Literal["ODCEncoder"] = "ODCEncoder"

    def __str__(self):
        return f"{self.__class__.__name__}({self.match})"

    def __post_init__(self):
        if not self.one_file_per_granule:
            self.output_file = self.resolve_path(self.output_file)
            self.output_file.parent.mkdir(exist_ok=True, parents=True)
            self.output_file.unlink(missing_ok=True)

    def create_output_df(self, msg: TabularMessage) -> pd.DataFrame:
        output_data = {}
        for col in self.MARS_keys:
            try:
                output_data[col.name] = col.values(msg)
            except ValueError as e:
                print(col)
                raise e
        return pd.DataFrame(output_data)

    def encode(self, msg: TabularMessage | FinishMessage):
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

        # And encode the supplied data
        # logger.debug(f"Columns before encoding to ODC: {df.columns}")
        # logger.info(f"Encoded {msg} to {self.output}")
        additional_metadata = {
            "encoded_by": "obsproc",  # TODO: add git commit hash or version here
        }
        if msg.metadata:
            for key in ["source", "observation_variable"]:
                val = getattr(msg.metadata, key)
                if val is not None:
                    additional_metadata[key] = val

            if msg.metadata.time_slice is not None:
                additional_metadata["timeslice"] = str(msg.metadata.time_slice.start_time.isoformat())

            additional_metadata.update(msg.metadata.unstructured)

        if self.one_file_per_granule:
            f = "data/outputs/{source}/odb/{observation_variable}/meteotracker_{date}_{time:04d}.odb"
            kwargs = dict(
                observation_variable=msg.metadata.observation_variable,
                source=msg.metadata.source,
                date=output_df["date@hdr"][0],
                time=output_df["time@hdr"][0],
            )
            f = f.format(**kwargs)

            self.output_file = Path(f).resolve()
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

                codec = select_codec(col.name, data, col.dtype, None)
                logger.debug(f"Chose codec {codec} for {col.name}, {data.dtype=}, {col.dtype=}")
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

        yield FileMessage(
            metadata=replace(msg.metadata, filepath=self.output_file),
        )


encoder = ODCEncoder
