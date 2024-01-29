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
from .construct_mars_request import construct_mars_request


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


def make_mars_request(verb, request):
    with tempfile.NamedTemporaryFile() as cmd, tempfile.NamedTemporaryFile() as output:
        if verb in {"list", "retrieve"}:
            request = request | dict(target=output.name)

        mars_request_string = write_temp_mars_request(request, file=cmd.name, verb=verb)
        # logger.debug(f"mars_request_string: {mars_request_string}")

        try:
            stderr = sb.check_output(["mars", cmd.name], stderr=sb.STDOUT, encoding="utf-8")
        except sb.CalledProcessError as exc:
            logger.debug(f"Status : FAIL, retcode: {exc.returncode}, output: {exc.output}")
            raise CalledProcessError(f"MARS {verb} returned error: {exc.output}")

        return stderr, output.read().decode("utf-8")


def mars_archive(request, source):
    stderr, output = make_mars_request("archive", request | {"source": str(source)})
    return stderr


def mars_list(request):
    request = request | dict(output="cost")
    stderr, output = make_mars_request("list", request)
    entries = next(l for l in output.split("\n") if "Entries" in l)
    n = int(entries.split(":")[1].strip())
    return dict(entries=n)


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

        mars_request = message.metadata.mars_request.as_strings()
        # logger.info(f"mars_request: {mars_request}")

        entries = mars_list(mars_request)["entries"]
        logger.debug(f"Got {entries} entries")

        if entries == 0 or self.globals.overwrite:
            logger.debug(f"Archiving via mars client")
            mars_archive(mars_request, source=message.metadata.filepath)
        else:
            # Cut this message off so that it doesn't overwrite data
            logger.debug(f"Dropping data because it's already in the database.")
            return

        # Send a notification to AVISO that we put this data into the DB
        response = send_aviso_notification(mars_request)
        # logger.debug("Aviso respose {response}")

        # TODO: the explicit mars_keys should not be necessary here.
        metadata = self.generate_metadata(message, mars_keys=message.metadata.mars_keys)
        output_msg = FileMessage(metadata=metadata)

        assert output_msg.metadata.mars_keys is not None
        yield self.tag_message(output_msg, message)
