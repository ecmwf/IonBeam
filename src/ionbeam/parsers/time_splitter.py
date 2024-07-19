import pandas as pd
import dataclasses
from typing import Iterable
import zipfile

from ..core.bases import Processor, FileMessage, FinishMessage, TabularMessage

import logging

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class TimeSplitter(Processor):
    "Data into 1h time chunks"
    granularity: str = "6H"
    finish_after: int | None = None

    def process(self, input_message: TabularMessage | FinishMessage) -> Iterable[TabularMessage]:
        if isinstance(input_message, FinishMessage):
            return

        chunks = 0
        for key, chunk in input_message.data.resample(rule = self.granularity, on = "time"):
            metadata = self.generate_metadata(input_message, filepath=None)
            output_msg = TabularMessage(metadata=metadata, data=chunk)
            yield self.tag_message(output_msg, input_message)
            chunks += 1
            if self.finish_after is not None and chunks > self.finish_after:
                break



