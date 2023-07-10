from typing import Iterable, List, Literal

import pandas as pd

import dataclasses

from ..core.bases import FileMessage, TabularMessage, MetaData, FinishMessage, Parser, InputColumn

from ..core.converters import unit_conversions

import logging

logger = logging.getLogger(__name__)

# @todo - Processing of the various different types should be based on this class, or on derived types
#         built in a factory. The work done in CSVParser is a bit yucky...


@dataclasses.dataclass
class CSVParser(Parser):
    identifying_columns: List[InputColumn]
    value_columns: List[InputColumn]
    metadata_columns: List[InputColumn] = dataclasses.field(default_factory=list)
    separator: str = ","
    custom_nans: List[str] | None = None

    name: Literal["CSVParser"] = "CSVParser"

    def __str__(self):
        return f"{self.__class__.__name__}({self.match})"

    def __post_init__(self):
        self.all_columns = self.value_columns + self.metadata_columns + self.identifying_columns
        self.fixed_columns = self.identifying_columns + self.metadata_columns
        self.columns_mapping = {c.key: c.name for c in self.all_columns}

    def format_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        extra_keys = {k for k in df.columns if k not in self.columns_mapping}
        if extra_keys:
            logger.warning(f"Data keys [{','.join(extra_keys)}] do not exist in the config!")

        df.rename(columns=self.columns_mapping, inplace=True)

        # Do dtype conversions
        # hopefully this was already done by read_csv but you never know
        for col in self.all_columns:
            if col.name in df and col.canonical_variable.dtype is not None:
                # convert any custom nan values to the string "NaN"
                if self.custom_nans is not None and col.canonical_variable.dtype.startswith("float"):
                    for nan in self.custom_nans:
                        df[col.name].replace(nan, "NaN", inplace=True)

                try:
                    df[col.name] = df[col.name].astype(col.canonical_variable.dtype, copy=False)
                except ValueError as e:
                    raise ValueError(f"{col.name} conversion to {col.canonical_variable.dtype} failed with error {e}")

        # Do unit conversions
        for col in self.all_columns:
            if col.name in df and col.unit != col.canonical_variable.unit:
                converter = unit_conversions[f"{col.unit.strip()} -> {col.canonical_variable.unit.strip()}"]
                df[col.name] = df[col.name].apply(converter)

        return df

    def split_columns(self, df: pd.DataFrame) -> Iterable[pd.DataFrame]:
        for value_col in self.value_columns:
            if value_col.name in df.columns and value_col.canonical_variable.output:
                output_cols = self.fixed_columns + [
                    value_col,
                ]
                yield value_col.name, df[[c.name for c in output_cols]]

    def process(self, rawdata: FileMessage | FinishMessage) -> Iterable[TabularMessage]:
        if isinstance(rawdata, FinishMessage):
            return

        # Ensure column names match what we would like
        assert rawdata.metadata.filepath is not None
        df = pd.read_csv(
            rawdata.metadata.filepath,
            sep=self.separator,
        )
        df = self.format_dataframe(df)

        # Split the data into data frames for each of the value types
        for variable_name, df in self.split_columns(df):
            assert isinstance(variable_name, str)
            yield TabularMessage(
                data=df,
                metadata=MetaData(
                    source=rawdata.metadata.source,
                    observation_variable=variable_name,
                    time_slice=None,  # don't know this yet
                    filepath=rawdata.metadata.filepath,
                ),
            )


parser = CSVParser
