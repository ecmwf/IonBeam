# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

from typing import Iterable, List, Literal

import pandas as pd

import dataclasses

from ..core.bases import (
    Parser,
    FileMessage,
    TabularMessage,
    MetaData,
    FinishMessage,
    InputColumn,
)

from ..core.converters import unit_conversions
from ..core.html_formatters import make_section, action_to_html, dataframe_to_html
from dataclasses import asdict

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

    def __str__(self):
        return f"{self.__class__.__name__}({self.match})"

    def _repr_html_(self):
        column_html = dataframe_to_html(
            pd.DataFrame.from_records(asdict(c) for c in self.all_columns),
            max_rows=200,
        )
        extra_sections = [
            make_section("Columns", column_html, open=False),
        ]
        return action_to_html(self, extra_sections=extra_sections)

    def init(self, globals):
        super().init(globals)
        self.all_columns = self.value_columns + self.metadata_columns + self.identifying_columns
        self.fixed_columns = self.identifying_columns + self.metadata_columns
        self.columns_mapping = {c.key: c.name for c in self.all_columns}

        canonical_variables = {c.name: c for c in globals.canonical_variables}
        for col in self.all_columns:
            # We are explicitly ignoring this input key
            if col.discard:
                continue

            # Check that preprocessors only create columns with canonical names
            if col.name not in canonical_variables:
                raise ValueError(f"{col.name} not in canonical names!")

            # Just copy the whole canonical variable object onto it
            col.canonical_variable = canonical_variables[col.name]

            # Emit a warning if the config doesn't include the unit conversion
            if (
                col.unit
                and col.unit != col.canonical_variable.unit
                and f"{col.unit.strip()} -> {col.canonical_variable.unit.strip()}" not in unit_conversions
            ):
                logger.warning(
                    f"No unit conversion registered for {col.name} {col.unit} -> {col.canonical_variable.unit}|"
                )

            # Tell the parsers what dtypes they need to use
            col.dtype = canonical_variables[col.name].dtype

    def format_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # extra_keys = {k for k in df.columns if k not in self.columns_mapping}
        # if extra_keys:
        #     print(extra_keys)
        #     logger.warning(f"Data keys {','.join(extra_keys)} do not exist in the config!")

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

    def split_columns(self, df: pd.DataFrame):
        for value_col in self.value_columns:
            if value_col.name in df.columns and value_col.canonical_variable.output:
                output_cols = self.fixed_columns + [
                    value_col,
                ]
                yield value_col, df[[c.name for c in output_cols]]

    def process(self, rawdata: FileMessage | FinishMessage) -> Iterable[TabularMessage]:
        if isinstance(rawdata, FinishMessage):
            return

        if hasattr(rawdata, "data"):
            df = rawdata.data

        elif rawdata.metadata.filepath is not None:
            assert rawdata.metadata.filepath
            df = pd.read_csv(
                rawdata.metadata.filepath,
                sep=self.separator,
            )
        else:
            raise ValueError(
                f"Inappropriate message type passed to CSVParser: {rawdata}"
                " This can also happen if you hot reload the code in a notebook, try restarting the kernel."
            )

        df = self.format_dataframe(df)

        # Split the data into data frames for each of the value types
        for variable_column, df in self.split_columns(df):
            metadata = self.generate_metadata(
                message=rawdata,
                observation_variable=variable_column.name,
                filepath=None,
            )

            output_msg = TabularMessage(
                metadata=metadata,
                columns=[c.canonical_variable for c in self.fixed_columns + [variable_column]],
                data=df,
            )

            yield self.tag_message(output_msg, rawdata)


parser = CSVParser
