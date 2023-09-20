import pandas as pd
import dataclasses
from typing import Iterable

from ..core.bases import Processor, FileMessage, FinishMessage, TabularMessage

import logging

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class CSVChunker(Processor):
    "Split large CSV files into more manageable chunks"
    rows_per_chunk: int = 10_000
    separator: str = ","

    def __str__(self):
        return f"{self.__class__.__name__}({self.match})"

    def process(
        self, input_message: FileMessage | FinishMessage
    ) -> Iterable[TabularMessage]:
        logger.debug(f"CSV Chunker got {input_message}")
        if isinstance(input_message, FinishMessage):
            return
        assert input_message.metadata.filepath is not None

        with pd.read_csv(
            input_message.metadata.filepath, chunksize=self.rows_per_chunk, sep=";"
        ) as reader:
            for chunk in reader:
                metadata = self.generate_metadata(input_message, filepath=None)
                output_msg = TabularMessage(metadata=metadata, data=chunk)
                yield self.tag_message(output_msg, input_message)
