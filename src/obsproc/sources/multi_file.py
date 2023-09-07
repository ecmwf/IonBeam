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

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class MultiFileSource(Source):
    source: str
    paths: List[str]
    basepath: str = "."
    finish_after: int | None = None
    name: Literal["MultiFileSource"] = "MultiFileSource"

    def __post_init__(self):
        # can't make self.basepath type Path because dataclass wizard doesn't support loading in Path objects (yet)
        self.basepath = self.resolve_path(self.basepath)

    def generate(self):
        emitted_messages = 0
        for pattern in self.paths:
            pattern = Path(pattern)

            if pattern.is_absolute():
                raise ValueError("Absolute paths patterns are not supported, use basepath.")

            # make the pattern absolute so that we can check if it exists as a file or folder
            pattern = self.basepath / pattern

            if pattern.is_file():
                paths = [
                    pattern,
                ]
            elif pattern.is_dir():
                paths = (p for p in pattern.iterdir() if p.is_file())
            else:
                paths = self.basepath.glob(str(pattern.relative_to(self.basepath)))

            # Make the paths relative again and strip out directories
            paths = (p for p in paths if p.is_file() and not p.name.startswith("."))

            if not paths:
                logger.warning(f"Specified path pattern '{pattern}' does not exist. Skipping.")

            for path in paths:
                yield FileMessage(
                    metadata=MetaData(
                        source=self.source,
                        observation_variable=None,  # don't know this yet
                        time_slice=None,  # don't know this yet
                        filepath=path,
                    ),
                )
                emitted_messages += 1
                if self.finish_after is not None and emitted_messages >= self.finish_after:
                    return


source = MultiFileSource
