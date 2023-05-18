import os
from queue import Queue
from typing import Iterable

from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer

from . import RawData, Source


class WatchDirectorySource(PatternMatchingEventHandler, Source):
    # @todo: Provide a sensible way of stopping the observer threads...

    def __init__(
        self,
        path: str = None,
        paths: Iterable[str] = None,
        include_dirname=False,
        patterns=None,
        ignore_patterns=None,
        ignore_directories=False,
        case_sensitive=True,
    ):
        """
        :param path: The directory to watch
        :param paths: A list of directories to watch
        :param include_dirname: Include the name of the watched directory when determining the id
        :param patterns: Patterns to match against (according to watchdog configuration strategy)
        :param ignore_patterns: Specify sub-paths to ignore (according to watchdog configuration strategy)
        :param ignore_directories: Specify sub-path directories to ignore (according to watchdog configuration strategy)
        :param case_sensitive: Specify if pattern matching is case sensitive
        """
        super().__init__(patterns, ignore_patterns, ignore_directories, case_sensitive)

        assert path is not None or paths is not None
        self.paths = [
            os.path.abspath(p) for p in ([path] if path is not None else paths)
        ]
        self.include_dirname = include_dirname

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

    def __str__(self):
        return f"MultiFileSource({self.paths})"

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
        self.queue.put(RawData(event.src_path, id))

    def generate(self):
        while self.queue:
            raw_data = self.queue.get()
            self.queue.task_done()
            yield raw_data


source = WatchDirectorySource
