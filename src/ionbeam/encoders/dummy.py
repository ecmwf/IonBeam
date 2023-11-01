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
from typing import Literal

from ..core.bases import Message, Encoder


@dataclasses.dataclass
class DummyEncoder(Encoder):
    match: str = "all"

    def matches(self, message: Message) -> bool:
        return True

    def encode(self, data: Message):
        yield None


encoder = DummyEncoder
