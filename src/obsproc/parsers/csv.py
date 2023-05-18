import itertools
from typing import Generator

import pandas as pd

from ..sources import RawData
from . import ParsedData, Parser

# @todo - Processing of the various different types should be based on this class, or on derived types
#         built in a factory. The work done in CSVParser is a bit yucky...


class ColDetails:
    def __init__(
        self,
        name=None,
        title=None,
        type="double",
        unit=None,
        identifying=False,
        date_format=None,
    ):
        self.name = name or title
        self.typ = type
        self.unit = unit
        self.identifying = identifying
        self.date_format = date_format


class CSVParser(Parser):
    def __init__(self, metadata_columns, value_columns, header=False, **kwargs):
        """
        Construct a CSV parser
        :param header: Will the parsed data have a header row?
        """
        assert header, "We haven't implemented/checked non-labelled data yet"

        all_columns = metadata_columns + value_columns

        self.columns_mapping = {
            c["title"]: c["name"] for c in all_columns if "title" in c and "name" in c
        }

        self.identifying_columns = {
            col["name"] if "name" in col else col["title"]: ColDetails(**col)
            for col in metadata_columns
            if col.get("identifying", False)
        }
        self.metadata_columns = {
            col["name"] if "name" in col else col["title"]: ColDetails(**col)
            for col in metadata_columns
            if not col.get("identifying", False)
        }
        self.value_columns = {
            col["name"] if "name" in col else col["title"]: ColDetails(**col)
            for col in value_columns
        }

        self.datetime_cols = {
            col["name"] if "name" in col else col["title"]: col.get("date_format", None)
            for col in itertools.chain(metadata_columns, value_columns)
            if col.get("type", None) == "datetime"
        }

        super().__init__(**kwargs)

    def __str__(self):
        return f"CSVParser()"

    def parse(self, rawdata: RawData) -> Generator[ParsedData, None, None]:
        # Ensure column names match what we would like

        df = pd.read_csv(rawdata.location)
        df.rename(columns=self.columns_mapping, inplace=True)

        # Patch any datetime columns to datetime objects

        for col_name, date_format in self.datetime_cols.items():
            df[col_name] = pd.to_datetime(df[col_name], format=date_format, utc=True)

        # Modify any columns as needed

        fixed_output_cols = list(self.identifying_columns.keys()) + list(
            self.metadata_columns.keys()
        )

        # Split the data into data frames for each of the value types

        for value_col in self.value_columns.keys():
            output_cols = fixed_output_cols + [value_col]
            yield ParsedData(df[output_cols], {})


parser = CSVParser
