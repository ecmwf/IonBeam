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
    FinishMessage,
    InputColumns,
)
from unicodedata import normalize

from ..core.converters import unit_conversions
from ..core.html_formatters import make_section, action_to_html, dataframe_to_html
from dataclasses import asdict

import logging

logger = logging.getLogger(__name__)

# @todo - Processing of the various different types should be based on this class, or on derived types
#         built in a factory. The work done in CSVParser is a bit yucky...

# TODO: split the functionality of this up into:
#     - parse CSV files 
#     - rename columns
#     - split output into one message per variable

@dataclasses.dataclass
class Splitter(Parser):
    """
    """
    identifying_keys: List[str] = dataclasses.field(default_factory=list)
    metadata_keys: List[str] = dataclasses.field(default_factory=list)

    def init(self, globals):
        super().init(globals)
        self.fixed_column_names = self.metadata_keys + self.identifying_keys
        self.canonical_variables_map = {c.name: c for c in self.globals.canonical_variables}

    def split_columns(self, df: pd.DataFrame):
        for column_name in df.columns:
            if column_name in self.fixed_column_names: continue
            yield column_name, df[self.fixed_column_names + [column_name]]


    def process(self, rawdata: TabularMessage | FinishMessage) -> Iterable[TabularMessage]:
        if isinstance(rawdata, FinishMessage):
            return

        df = rawdata.data
        # Split the data into data frames for each of the value types
        for variable_column, df in self.split_columns(df):
            metadata = self.generate_metadata(
                message=rawdata,
                observation_variable=variable_column,
                filepath=None,
            )

            output_msg = TabularMessage(
                metadata=metadata,
                columns=[self.canonical_variables_map[c] for c in self.fixed_column_names + [variable_column]],
                data=df,
            )

            yield self.tag_message(output_msg, rawdata)
