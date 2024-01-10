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
from pathlib import Path

import pandas as pd

import dataclasses

from ..core.bases import Writer, Message, FileMessage, FinishMessage

import logging
import findlibs
import os
import yaml
import json

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class FB5Config:
    type: str = "remote"
    host: str = "localhost"
    port: int = 7654
    dataPortStart: int = 40000
    dataPortCount: int = 100
    schema: Path | None = None

    def asdict(self):
        d = dataclasses.asdict(self)
        d["schema"] = str(d["schema"])
        return d


@dataclasses.dataclass
class FDBWriter(Writer):
    FDB5_client_config: FB5Config
    debug: list[str] = dataclasses.field(default_factory=list)

    def __str__(self):
        return f"{self.__class__.__name__}()"

    def init(self, globals):
        super().init(globals)
        self.metadata = dataclasses.replace(self.metadata, state="written")

    def process(self, input_message: FileMessage | FinishMessage) -> Iterable[Message]:
        if isinstance(input_message, FinishMessage):
            return

        assert input_message.metadata.filepath is not None

        fdb5_path = findlibs.find("fdb5")
        logger.debug(f"FDBWriter using fdb5 shared library from {fdb5_path}")

        os.environ["FDB5_CONFIG"] = yaml.dump(self.FDB5_client_config.asdict())

        for lib in self.debug:
            os.environ[f"{lib.upper()}_DEBUG"] = "1"

        import pyfdb

        fdb = pyfdb.FDB()
        request = input_message.metadata.mars_request.as_strings()
        logger.debug(f"MARS key for fdb archive: {json.dumps(request, indent=4)}")

        with open(input_message.metadata.filepath, "rb") as f:
            fdb.archive(f.read(), request=request)

        metadata = self.generate_metadata(input_message, mars_request=input_message.metadata.mars_request)
        output_msg = FileMessage(metadata=metadata)
        yield self.tag_message(output_msg, input_message)
