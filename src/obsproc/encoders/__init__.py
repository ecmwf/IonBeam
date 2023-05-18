import os
from typing import Generator

from ..core.plugins import find_plugin
from ..parsers import ParsedData


class EncodedData:
    def __init__(self, format, data, metadata):
        self.format = format
        self.data = data
        self.metadata = metadata

    def __str__(self):
        return f"EncodedData(format={self.format})"


class Encoder:
    def encode(self, parsed_data: ParsedData) -> Generator[EncodedData, None, None]:
        raise NotImplementedError("Implement parse() in derived class")


def load_encoder(name: str, **kwargs) -> Encoder:
    klass = find_plugin(os.path.dirname(__file__), __name__, "encoder", name)
    return klass(**kwargs)
