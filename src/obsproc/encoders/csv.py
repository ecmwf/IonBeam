# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

import dataclasses
from typing import Literal, Iterable
from dataclasses import asdict

from ..core.bases import TabularMessage, FileMessage, FinishMessage, Encoder

from pathlib import Path


@dataclasses.dataclass
class CSVEncoder(Encoder):
    output: str
    seconds: bool = False
    minutes: bool = False
    one_file_per_granule: bool = True

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

        metadata = self.generate_metadata(
            msg, filepath=self.output_file, encoded_format="csv"
        )
        output_message = FileMessage(metadata=metadata)
        output_message = self.tag_message(output_message, msg)
        yield output_message


encoder = CSVEncoder
