# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

import logging

from ...core.bases import FileMessage, Source, MetaData
from typing import List
import dataclasses
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SmartCitizenKitSource(Source):
    secrets_file: Path
    cache_directory: Path

    "The time interval to ingest, can be overidden by globals.source_timespan"
    start_date: str
    end_date: str

    "How many messages to emit before stopping, useful to debug runs."
    finish_after: int | None = None

    "The value to insert as source"
    source: str = "smart_citizen_kit"

    def init(self, globals):
        super().init(globals)
        self.secrets_file = self.resolve_path(self.secrets_file, type="config")
        self.cache_directory = self.resolve_path(self.cache_directory, type="data")
        logger.debug("Smart Citizen Kit source initialised")
        logger.debug(f"Secrets file: {self.secrets_file}")

    def __str__(self):
        cls = self.__class__.__name__
        return f"{cls}({self.paths}, source = '{self.metadata.source}')"

    def generate(self):
        emitted_messages = 0
        data = ["dfgd", "d" ,"eg" , "e" ]
      
        for message in data:
                yield FileMessage(
                    metadata=self.generate_metadata(
                        filepath=message,
                    ),
                )
                emitted_messages += 1
                if self.finish_after is not None and emitted_messages >= self.finish_after:
                    return
