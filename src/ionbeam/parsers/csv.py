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
import logging
from dataclasses import asdict
from typing import Iterable, List
from unicodedata import normalize

import pandas as pd

from ..core.bases import (
    FileMessage,
    FinishMessage,
    InputColumns,
    Parser,
    TabularMessage,
)
from ..core.converters import unit_conversions
from ..core.html_formatters import action_to_html, dataframe_to_html, make_section

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class CSVParser(Parser):
    """
    Parse CSV files, rename their columns and split them into multiple messages based on observation variable.

    Args:
        identifying_columns: A list of columns that represent identifying information like lat, lon, altitude, time
        value_columns: A list of columns that represent measured values
        metadata_columns: A list of columns that represent metadata like author or station_id
        separator: The separator character to use.
        custom_nans: A list of custom NaN values to use.
    """

    # Map key names and paths within external data sources to internal canonical names
    mappings: InputColumns

    identifying_keys: List[str] = dataclasses.field(default_factory=list)
    metadata_keys: List[str] = dataclasses.field(default_factory=list)
    separator: str = ","
    custom_nans: List[str] | None = None

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
        self.mappings = [c for c in self.mappings if not c.discard]
        self.mapping_names = {c.name for c in self.mappings}
        self.mapping_keys = {c.key for c in self.mappings}

        # Check that the identifying and metadata keys are in the mappings
        for name in self.identifying_keys + self.metadata_keys:
            if name not in self.mapping_names:
                raise ValueError(
                    f"{name=} not in mappings, but specified in metadata_keys or identifying_keys of CSV parser"
                )

        # Sort the non-discard columns into metadata, identifying and value columns
        self.metadata_columns = []
        self.identifying_columns = []
        self.value_columns = []
        for column in self.mappings:
            if column.name in self.metadata_keys:
                self.metadata_columns.append(column)
            elif column.name in self.identifying_keys:
                self.identifying_columns.append(column)
            else:
                self.value_columns.append(column)

        self.all_columns = self.value_columns + self.metadata_columns + self.identifying_columns
        self.fixed_columns = self.identifying_columns + self.metadata_columns

        self.fixed_column_names = {c.name for c in self.fixed_columns}
        self.columns_mapping = {c.key: c.name for c in self.mappings}
        self.canonical_variables_map = {c.name: c for c in globals.canonical_variables}

        for col in self.all_columns:
            # Check that c.name is actually one of the canonical names
            if col.name not in self.canonical_variables_map:
                all = [col for col in self.all_columns if col.name not in self.canonical_variables_map]
                yaml = "".join(
                    f'- name: {col.name}\n  unit: "{col.unit}"\n\n' if col.unit else f"- name: {col.name}\n\n"
                    for col in all
                )
                raise ValueError(
                    f"{col.name} from {self} config is not in canonical names!\n"
                    f"Put this into canonical variables:\n{yaml}"
                )

            # Copy a reference to the canonical variable object onto it
            col.canonical_variable = self.canonical_variables_map[col.name]

            # Emit a warning if the config doesn't include the unit conversion
            # This normalize("NFKD", ...) business is because there are some unicode characters that look identical but have
            # different unicode code points. normalize puts the in some canonical form so they can be compared.
            # Hint: Try out "µ" == "μ"
            if (
                col.unit
                and col.unit != col.canonical_variable.unit
                and f"{col.unit.strip()} -> {col.canonical_variable.unit.strip()}" not in unit_conversions
            ):
                logger.warning(
                    f"No unit conversion registered for {col.name}: {col.unit} -> {col.canonical_variable.unit}|"
                )

            # Tell the parsers what dtypes they need to use
            col.dtype = self.canonical_variables_map[col.name].dtype

    def rename_columns(self, df):
        for col in df.columns:
            # All columns should either already have the correct name ("in self.columns_mapping.values()")
            # Or have a translation in self.columns_mapping
            if col not in self.mapping_names and col not in self.mapping_keys:
                if any(c.key == col and c.discard for c in self.mappings):
                    raise ValueError(
                        f"BUG: {col=} is marked as discard in mappings.yaml, but somehow got emitted from the source."
                    )
                raise ValueError(f"{col=} not in CSVParser columns mapping, does it have an entry in mappings.yaml?")
        df.rename(columns=self.columns_mapping, inplace=True)

    def convert_datatypes(self, df):
        for col in self.all_columns:
            if col.name in df and col.canonical_variable.dtype is not None:
                # convert any custom nan values to the string "NaN"
                if self.custom_nans is not None and col.canonical_variable.dtype.startswith("float"):
                    for nan in self.custom_nans:
                        df.replace(to_replace={col.name: {nan: "NaN"}}, inplace=True)

                try:
                    df[col.name] = df[col.name].astype(col.canonical_variable.dtype, copy=False)
                except ValueError as e:
                    raise ValueError(f"{col.name} conversion to {col.canonical_variable.dtype} failed with error {e}")

    def do_unit_conversions(self, df):
        # Do unit conversions
        for col in self.all_columns:
            if col.name in df and col.unit != col.canonical_variable.unit:

                if col.canonical_variable.unit is None:
                    raise ValueError(f"{col.canonical_variable=} unit is None!")
                if col.unit is None:
                    raise ValueError(f"{col=} unit is None!")

                converter = unit_conversions[
                    normalize("NFKD", f"{col.unit.strip()} -> {col.canonical_variable.unit.strip()}")
                ]
                df[col.name] = df[col.name].apply(converter)

    def format_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        self.rename_columns(df)
        self.convert_datatypes(df)
        self.do_unit_conversions(df)
        return df

    def split_columns(self, df: pd.DataFrame):
        for column_name in df.columns:
            if column_name in self.fixed_column_names:
                continue
            yield column_name, df[list(self.fixed_column_names) + [column_name]]

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
        logging.debug(f"Processed: {df.columns}")

        metadata = self.generate_metadata(
            message=rawdata,
            filepath=None,
        )

        output_msg = TabularMessage(
            metadata=metadata,
            columns=[c.canonical_variable for c in self.all_columns],
            data=df,
        )

        yield self.tag_message(output_msg, rawdata)

        # if self.globals.split_data_columns:
        #     # Split the data into data frames for each of the value types
        #     for variable_column, df in self.split_columns(df):
        #         metadata = self.generate_metadata(
        #             message=rawdata,
        #             observation_variable=variable_column.name,
        #             filepath=None,
        #         )

        #         output_msg = TabularMessage(
        #             metadata=metadata,
        #             columns=[c.canonical_variable for c in self.fixed_columns + [variable_column]],
        #             data=df,
        #         )

        #         yield self.tag_message(output_msg, rawdata)


parser = CSVParser
