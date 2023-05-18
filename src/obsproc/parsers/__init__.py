
import os
from typing import Dict, Generator

import pandas as pd

from ..core.plugins import find_plugin
from ..sources import RawData


class ParsedData:

    def __init__(self, dataframe: pd.DataFrame, metadata: Dict=None):
        self.df = dataframe
        self.metadata = metadata or dict()

    def __str__(self):
        return f"ParsedData({self.metadata})"


class Parser:

    def parse(self, rawdata: RawData) -> Generator[ParsedData, None, None]:
        raise NotImplementedError("Implement parse() in derived class")


def load_parser(name: str, **kwargs) -> Parser:
    klass = find_plugin(os.path.dirname(__file__), __name__, 'parser', name)
    return klass(**kwargs)
