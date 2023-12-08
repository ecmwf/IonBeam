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


logger = logging.getLogger(__name__)


def write_temp_mars_request(
    request,
    file,
    verb="archive",
    # quote_keys = {"source", "target", "filter"}
):
    keys = []
    for key, value in request.items():
        # if key in quote_keys:
        keys.append(f'{key}="{value}"')
        # else:
        #     keys.append(f'{key}={value}')

    keys = ",\n    ".join(keys)
    rendered = f"""{verb},
    {keys}"""

    with open(file, "w") as f:
        f.write(rendered)

    return rendered


def run_temp_mars_request(file):
    try:
        output = sb.check_output(["mars", "/home/math/temp_request.mars"], stderr=sb.STDOUT)
    except sb.CalledProcessError as exc:
        print("Status : FAIL", exc.returncode, exc.output.decode())
        raise exc
    # else:
    #     print("Output: \n{}\n".format(output))


def time_format(i):
    return f"{i:04d}"


mars_value_formatters = {
    "time": time_format,
}


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

        assert message.metadata.mars_keys is not None
        assert message.metadata.filepath is not None

        request = {"database": "fdbdev", "class": "rd", "source": message.metadata.filepath}

        odb_keys = {k.key: k.value for k in message.metadata.mars_keys if not k.reason == "Skipped"}
        request = odb_keys | request

        request = {k: mars_value_formatters.get(k, str)(v) for k, v in request.items()}

        with tempfile.NamedTemporaryFile() as fp:
            mars_request = write_temp_mars_request(request, file=fp.name)
            logger.debug(mars_request)
            run_temp_mars_request(file=fp.name)

        response = send_aviso_notification(request)

        # TODO: the explicit mars_keys should not be necessary here.
        metadata = self.generate_metadata(message, mars_keys=message.metadata.mars_keys)
        output_msg = FileMessage(metadata=metadata)

        assert output_msg.metadata.mars_keys is not None
        yield self.tag_message(output_msg, message)
