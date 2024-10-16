from ..core.bases import Processor, TabularMessage, FinishMessage
from typing import Iterable
import dataclasses

import logging

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class QualityControl(Processor):
    test_variable: str

    def __str__(self):
        return f"{self.__class__.__name__}({self.test_variable})"

    # Gets called by the config parsing code to create the action object
    def init(self, globals):
        super().init(globals)
        self.metadata = dataclasses.replace(self.metadata, state="quality_controlled")

    def process(self, incoming_message: TabularMessage) -> Iterable[TabularMessage]:
        # Stop everything and just return!
        if isinstance(incoming_message, FinishMessage):
            return

        # modify the csv data of the message
        new_data = incoming_message.data
        new_data["P1"] = 100

        # update any keys in the metadata we need to
        metadata = self.generate_metadata(
            message=incoming_message,
        )

        # construct the outgoing message
        output_message = TabularMessage(
            metadata=metadata,
            data=new_data,
        )

        # generate the history of the message and send it out!
        yield self.tag_message(output_message, previous_msg=incoming_message)
