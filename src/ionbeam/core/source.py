import dataclasses
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from .bases import Action, DataMessage, TabularMessage

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

    def perform_copy_metadata_columns(self, df : pd.DataFrame, source: dict) -> None:
        for column_name, column_key in self.copy_metadata_to_columns.items():
            if column_name in df:
                logger.warning(f"Copy_data_to_Column {column_name} already in the dataframe, skipping")
                continue

            value = recursive_get(source, column_key.split("."))

            if value is None:
                logger.warning(f"Column {column_key} not found in metadata, skipping. Possible keys are {source}")
            else:   
                # logger.debug(f"Extracting {column_key} into {column_name} with value {value}")
                df[column_name] = value

    @abstractmethod
    def generate(self) -> Iterable[TabularMessage]:
        pass