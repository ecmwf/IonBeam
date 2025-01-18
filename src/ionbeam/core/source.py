import dataclasses
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from .bases import Action, CanonicalVariable, DataMessage, RawVariable, TabularMessage

logger = logging.getLogger(__name__)

def recursive_get(dictionary, query):
    if dictionary is None or len(query) == 0: 
        return dictionary
    first, *rest = query
    new_dictionary = dictionary.get(first, None)
    return recursive_get(new_dictionary, rest)


@dataclass
class Source(Action, ABC):
    "An Action which only outputs messages."
    copy_metadata_to_columns: dict[str, str] = dataclasses.field(default_factory=dict, kw_only=True)

    def __iter__(self):
        return self

    def __next__(self):
        if not hasattr(self, "_generator"):
            self._generator = iter(self.generate())
        assert self._generator is not None
        return next(self._generator)
    
    def matches(self, message: DataMessage) -> bool:
        return False

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)
        assert "external_station_id" in self.copy_metadata_to_columns, "external_station_id must be copied to columns"

    def perform_copy_metadata_columns(self, df : pd.DataFrame, source: dict, columns : dict[str, RawVariable | CanonicalVariable]) -> None:
        for column_name, column_key in self.copy_metadata_to_columns.items():
            if column_name in df:
                logger.warning(f"Copy_data_to_Column {column_name} already in the dataframe, skipping")
                continue

            if column_name not in self.globals.canonical_variables_by_name:
                assert ValueError, f"Column {column_name} not found in canonical variables"

            value = recursive_get(source, column_key.split("."))

            if value is None:
                logger.warning(f"Column {column_key} not found in metadata, skipping. Possible keys are {source}")
            else:   
                # logger.debug(f"Extracting {column_key} into {column_name} with value {value = } {type(value) = }")
                df[column_name] = value
                columns[column_name] = self.globals.canonical_variables_by_name[column_name]

                # push pandas to use the newer string type than object for string columns
                if columns[column_name].dtype is not None:
                    df[column_name] = df[column_name].astype(columns[column_name].dtype)
                

    @abstractmethod
    def generate(self) -> Iterable[TabularMessage]:
        pass