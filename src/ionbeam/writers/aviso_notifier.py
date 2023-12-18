# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

from typing import Iterable, List, Literal

import pandas as pd

import dataclasses

from ..core.bases import Writer, Message, FileMessage, FinishMessage
from ..core.aviso import send_aviso_notification

import logging

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class AVISONotifier(Writer):
    def __str__(self):
        return f"{self.__class__.__name__}()"

    def init(self, globals):
        super().init(globals)
        self.metadata = dataclasses.replace(self.metadata, state="aviso_notified")

    def process(self, message: FileMessage | FinishMessage) -> Iterable[Message]:
        if isinstance(message, FinishMessage):
            return

        request = {"database": "fdbdev", "class": "rd"}
        odb_keys = {k.key: k.value for k in message.metadata.mars_keys}
        request = odb_keys | request
        request = {k: mars_value_formatters.get(k, str)(v) for k, v in request.items()}

        # Send a notification to AVISO that we put this data into the DB
        logger.debug("Sending to aviso {request}")
        # response = send_aviso_notification(request)
        # logger.debug("Aviso response {response}")

        # TODO: the explicit mars_keys should not be necessary here.
        metadata = self.generate_metadata(message, mars_keys=message.metadata.mars_keys)
        output_msg = FileMessage(metadata=metadata)

        assert output_msg.metadata.mars_keys is not None
        yield self.tag_message(output_msg, message)
