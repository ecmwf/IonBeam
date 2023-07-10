from ..core.bases import TabularMessage, Processor
from ..core.plugins import find_plugin
from typing import Iterable
import os
import dataclasses


@dataclasses.dataclass
class EncodedMessage(TabularMessage):
    format: str

    def __str__(self):
        return f"EncodedMessage(format={self.format})"


@dataclasses.dataclass
class Encoder(Processor):
    def encode(self, parsed_data: TabularMessage) -> Iterable[EncodedMessage]:
        raise NotImplementedError("Implement encode() in derived class")

    def process(self, parsed_data: TabularMessage) -> Iterable[EncodedMessage]:
        return self.encode(parsed_data)


def load_encoder(name: str, **kwargs) -> Encoder:
    klass = find_plugin(os.path.dirname(__file__), __name__, "encoder", name)
    return klass(**kwargs)
