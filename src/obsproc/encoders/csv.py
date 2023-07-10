import dataclasses
from typing import Literal, Iterable
from dataclasses import asdict

from ..core.bases import TabularMessage, FileMessage, FinishMessage
from .bases import Encoder

from pathlib import Path


@dataclasses.dataclass
class CSVEncoder(Encoder):
    output: str
    seconds: bool = False
    minutes: bool = False
    one_file_per_granule: bool = True

    name: Literal["CSVEncoder"] = "CSVEncoder"

    def __str__(self):
        return f"{self.__class__.__name__}({self.match})"

    def __post_init__(self):
        if not self.one_file_per_granule:
            self.output_file = Path(self.output).resolve()

    def encode(self, msg: TabularMessage | FinishMessage) -> Iterable[FileMessage]:
        if isinstance(msg, FinishMessage):
            return
        if self.one_file_per_granule:
            f = self.output.format(**asdict(msg.metadata)).replace(" ", "_")
            self.output_file = Path(f).resolve()
            self.output_file.parent.mkdir(parents=True, exist_ok=True)

        msg.data.to_csv(self.output_file, index=False)

        yield FileMessage(
            metadata=dataclasses.replace(msg.metadata, filepath=self.output_file),
        )


encoder = CSVEncoder
