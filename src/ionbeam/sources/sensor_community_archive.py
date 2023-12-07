# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

import logging

from ..core.bases import FileMessage, Source, MetaData
from typing import Literal, List
import dataclasses
from pathlib import Path
from zipfile import ZipFile, is_zipfile

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class MultiFileSource(Source):
    path: Path = Path(".")
    finish_after: int | None = None

    def init(self, globals):
        super().init(globals)
        self.path = self.resolve_path(self.path, type="data")
        logger.debug(f"{self.__class__.__name__}: resolved path to {self.path}")

    def __str__(self):
        cls = self.__class__.__name__
        return f"{cls}({self.path}, source = '{self.metadata.source}')"

    def generate(self):
        emitted_messages = 0
        for month_dir in self.path.iterdir():
            if month_dir.is_file(): continue
            for zippath in month_dir.iterdir():
                if not is_zipfile(zippath): continue
                with ZipFile(p) as zip:
                    



            for path in paths:
                yield FileMessage(
                    metadata=self.generate_metadata(
                        filepath=path,
                    ),
                )
                emitted_messages += 1
                if self.finish_after is not None and emitted_messages >= self.finish_after:
                    return


source = MultiFileSource
