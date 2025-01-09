import dataclasses
import logging
import zipfile
from typing import Iterable

import pandas as pd

from ..core.bases import FileMessage, FinishMessage, Processor, TabularMessage

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class CSVChunker(Processor):
    "Split large CSV files into more manageable chunks"
    rows_per_chunk: int = 10_000
    finish_after: int | None = None
    separator: str = ","

    def process(self, input_message: FileMessage | FinishMessage) -> Iterable[TabularMessage]:
        logger.debug(f"CSV Chunker got {input_message}")

        assert input_message.metadata.filepath is not None

        chunks = 0

        def yield_chunks(file, chunks):
            with pd.read_csv(file, chunksize=self.rows_per_chunk, sep=self.separator) as reader:
                for chunk in reader:
                    metadata = self.generate_metadata(input_message, filepath=None)
                    output_msg = TabularMessage(metadata=metadata, data=chunk)
                    yield self.tag_message(output_msg, input_message)
                    chunks += 1
                    if self.finish_after is not None and chunks > self.finish_after:
                        break
        
        # If it's a zip file, look at everything inside
        if zipfile.is_zipfile(input_message.metadata.filepath):
            with zipfile.ZipFile(input_message.metadata.filepath) as zip:
                for file_info in zip.infolist():
                    if file_info.filename.endswith("csv"):
                        with zip.open(file_info) as f:
                            for msg in yield_chunks(f, chunks):
                                yield msg
                    else:
                        logger.warning(f"CSVChunker found non-csv files inside zips: "
                                       f"   zip: {input_message.metadata.filepath}   "
                                       f"   file: {file_info.filename}               ")

        elif input_message.metadata.filepath.suffix == ".csv":
            yield_chunks(input_message.metadata.filepath, chunks)
        
        else:
            logger.warning(f"CSVChunker got unknown filetype: {input_message.metadata.filepath}")



