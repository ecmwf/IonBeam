import dataclasses
import logging
from typing import Iterable

from ..core.bases import FinishMessage, Processor, TabularMessage

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class TimeSplitter(Processor):
    "Data into 1h time chunks"
    granularity: str = "6H"
    finish_after: int | None = None

    def process(self, input_message: TabularMessage | FinishMessage) -> Iterable[TabularMessage]:


        chunks = 0
        for key, chunk in input_message.data.resample(rule = self.granularity, on = "time"):
            metadata = self.generate_metadata(input_message, filepath=None)
            output_msg = TabularMessage(metadata=metadata, data=chunk)
            yield self.tag_message(output_msg, input_message)
            chunks += 1
            if self.finish_after is not None and chunks > self.finish_after:
                break



