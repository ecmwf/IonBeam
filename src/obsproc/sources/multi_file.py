
import logging
import os
from typing import Iterable

from . import RawData, Source


class MultiFileSource(Source):

    def __init__(self, paths: Iterable[str], basepath=None):
        self.paths = paths
        self.basepath = basepath or os.getcwd()

    def __str__(self):
        return f"MultiFileSource({self.paths})"

    def generate(self):
        for p in self.paths:
            if os.path.exists(p):
                if os.path.isdir(p):
                    for root, dirs, files in os.walk(p):
                        for f in files:
                            new_p = os.path.realpath(os.path.join(root, f))
                            yield RawData(new_p, os.path.relpath(new_p, self.basepath))
                else:
                    new_p = os.path.realpath(p)
                    yield RawData(new_p, os.path.relpath(new_p, self.basepath))
            else:
                logging.warning(f"Specified path '{p}' does not exist. Skipping.")


source = MultiFileSource