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
import os
from queue import Queue
from typing import List

from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer

from ..core.bases import FileMessage
from ..core.source import Source


@dataclasses.dataclass
class WatchDirectorySource(PatternMatchingEventHandler, Source):
    """
    Init parameters:
        :param paths: A list of directories to watch
        :param include_dirname: Include the name of the watched directory when determining the id
        :param patterns: Patterns to match against (according to watchdog configuration strategy)
        :param ignore_patterns: Specify sub-paths to ignore (according to watchdog configuration strategy)
        :param ignore_directories: Specify sub-path directories to ignore (according to watchdog configuration strategy)
        :param case_sensitive: Specify if pattern matching is case sensitive
    """

    paths: List[str] = dataclasses.field(default_factory=list)
    patterns: str | List[str] | None = None
    include_dirname: bool = False
    ignore_patterns: List[str] | None = None
    ignore_directories: bool = False
    case_sensitive: bool = True
    # @todo: Provide a sensible way of stopping the observer threads...

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)

        self.paths = [os.path.abspath(p) for p in self.paths]
        # The output queue

        self.queue = Queue()

        # Start the observer threads

        self.observer = Observer()
        for path in self.paths:
            self.observer.schedule(self, path, recursive=True)
        self.observer.start()

        # Somewhere we need an:
        # observer.stop()
        # observer.join()

    def on_created(self, event):
        data_path = os.path.abspath(event.src_path)

        basepath = None
        for p in self.paths:
            if data_path.startswith(p + os.sep):
                basepath = p
                break
        assert basepath is not None

        if self.include_dirname:
            basepath = os.path.dirname(basepath)

        id = os.path.relpath(data_path, basepath)
        self.queue.put(FileMessage(event.src_path, id))

    def generate(self):
        while self.queue:
            raw_data = self.queue.get()
            self.queue.task_done()
            yield raw_data


source = WatchDirectorySource
