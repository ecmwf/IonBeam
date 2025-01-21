#
# (C) Copyright 2023 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#

import dataclasses
import logging
from typing import Iterable

from ..core.bases import (
    Parser,
    TabularMessage,
)

logger = logging.getLogger(__name__)


def recursive_get(dictionary, query):
    if dictionary is None or len(query) == 0: return dictionary
    first, *rest = query
    new_dictionary = dictionary.get(first, None)
    return recursive_get(new_dictionary, rest)

@dataclasses.dataclass
class ExtractMetaData(Parser):
    """
    Extra data from unstructured JSON metadata into 
    """

    "A list of data to extract from the incoming API responses and add in as columns in the data"
    copy_metadata_to_columns: dict[str, str]
    copy_to_metadata: dict[str, str]

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)


    def process(self, message: TabularMessage) -> Iterable[TabularMessage]:
        # So we can modify the data without modifying the original message
        message.data = message.data.copy()
        
        for column_name, column_key in self.copy_metadata_to_columns.items():

            if column_name in message.data.columns:
                logger.warning(f"Column {column_name} already in the dataframe, skipping")
                continue

            value = recursive_get(message.metadata.unstructured, column_key.split("."))
            if value is None:
                logger.warning(f"Column {column_key} not found in metadata, skipping. Possible keys are {message.metadata.unstructured}")
            if column_name in message.data.columns:
                logger.warning(f"Column {column_name} already in the dataframe, skipping")
            else:   
                # logger.debug(f"Extracting {column_key} into {column_name} with value {value}")
                message.data[column_name] = value

        for name, key in self.copy_to_metadata.items():
            value = recursive_get(message.metadata.unstructured, key.split("."))
            if value is None:
                logger.warning(f"Key {key} not found in data, skipping. Possible keys are {message.metadata.unstructured}")
            else:
                setattr(message.metadata, name, value)

        output_msg = TabularMessage(
            metadata=self.generate_metadata(
                message=message,
            ),
            data=message.data,
        )

        yield output_msg
