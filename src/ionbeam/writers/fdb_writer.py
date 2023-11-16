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

from ..core.bases import Writer, Message, FileMessage, FinishMessage

import logging

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class FDBWriter(Writer):
    def __str__(self):
        return f"{self.__class__.__name__}()"

    def init(self, globals):
        super().init(globals)
        self.metadata = dataclasses.replace(self.metadata, state="written")

    def process(self, input_message: FileMessage | FinishMessage) -> Iterable[Message]:
        logger.debug(f"CSV Chunker got {input_message}")
        if isinstance(input_message, FinishMessage):
            return

        assert input_message.metadata.filepath is not None
        logger.debug("FDB Writer got message with metadata {input_message.metadata}")

        metadata = self.generate_metadata(input_message)
        output_msg = FileMessage(metadata=metadata)
        yield self.tag_message(output_msg, input_message)
