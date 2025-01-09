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
import logging
from typing import Iterable

from ..core.bases import Parser, TabularMessage

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DropEmpty(Parser):
    """

    """

    def process(self, msg: TabularMessage) -> Iterable[TabularMessage]:
        if len(msg.data) == 0:
            return
        
        metadata = self.generate_metadata(
            message=msg,
        )

        output_msg = TabularMessage(
            metadata=metadata,
            data=msg.data,
        )

        yield self.tag_message(output_msg, msg)
