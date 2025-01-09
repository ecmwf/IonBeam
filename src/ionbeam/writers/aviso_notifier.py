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

from ..core.aviso import send_aviso_notification
from ..core.bases import FileMessage, Message, Writer

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class AVISONotifier(Writer):
    def __str__(self):
        return f"{self.__class__.__name__}()"

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)
        self.metadata = dataclasses.replace(self.metadata, state="aviso_notified")

    def process(self, message: FileMessage) -> Iterable[Message]:
        request = {"database": "fdbdev", "class": "rd"}
        request |= message.metadata.mars_id.as_strings()

        # Send a notification to AVISO that we put this data into the DB
        logger.debug(f"Sending to aviso {request}")
        response = send_aviso_notification(request)
        logger.debug(f"Aviso response {response}")

        # TODO: the explicit mars_keys should not be necessary here.
        metadata = self.generate_metadata(message, mars_id=message.metadata.mars_id)
        output_msg = FileMessage(metadata=metadata)

        assert output_msg.metadata.mars_id is not None
        yield self.tag_message(output_msg, message)
