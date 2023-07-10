import dataclasses
from typing import Literal

from ..core.bases import Message
from .bases import Encoder


@dataclasses.dataclass
class DummyEncoder(Encoder):
    match: str = "all"
    name: Literal["DummyEncoder"] = "DummyEncoder"

    def matches(self, message: Message) -> bool:
        return True

    def encode(self, data: Message):
        yield None


encoder = DummyEncoder
