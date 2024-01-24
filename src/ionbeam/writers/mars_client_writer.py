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


import subprocess as sb
from tqdm.notebook import tqdm
import tempfile
import sys


logger = logging.getLogger(__name__)


def write_temp_mars_request(request, file, verb="archive", quote_keys={"source", "target", "filter"}):
    keys = []
    for key, value in request.items():
        if key in quote_keys:
            keys.append(f'{key}="{value}"')
        else:
            keys.append(f"{key}={value}")

    keys = ",\n    ".join(keys)
    rendered = f"""{verb},
    {keys}"""

    with open(file, "w") as f:
        f.write(rendered)

    return rendered


def run_temp_mars_request(file):
    try:
        output = sb.check_output(["mars", file], stderr=sb.STDOUT)
    except sb.CalledProcessError as exc:
        print("Status : FAIL", exc.returncode, exc.output.decode())
        raise exc
    else:
        logger.debug("MARS Archive Output: \n{}\n".format(output))


@dataclasses.dataclass
class MARSWriter(Writer):
    def __str__(self):
        return f"{self.__class__.__name__}()"

    def init(self, globals):
        super().init(globals)
        self.metadata = dataclasses.replace(self.metadata, state="written")

    def process(self, message: FileMessage | FinishMessage) -> Iterable[Message]:
        if isinstance(message, FinishMessage):
            return

        assert message.metadata.mars_request is not None
        assert message.metadata.filepath is not None

        request = {"database": "fdbdev", "class": "rd", "source": str(message.metadata.filepath)}
        mars_request = request | message.metadata.mars_request.as_strings()

        logger.info(f"mars_request: {mars_request}")

        with tempfile.NamedTemporaryFile() as fp:
            mars_request_string = write_temp_mars_request(mars_request, file=fp.name)
            logger.debug(f"mars_request_string: {mars_request_string}")
            # run_temp_mars_request(file=fp.name)
            sys.exit()

        # TODO: the explicit mars_keys should not be necessary here.
        metadata = self.generate_metadata(message, mars_request=message.metadata.mars_request)
        output_msg = FileMessage(metadata=metadata)

        assert output_msg.metadata.mars_request is not None
        yield self.tag_message(output_msg, message)
