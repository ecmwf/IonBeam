from pathlib import Path


from ..core.plugins import find_plugin
from ..core.bases import Parser

from .csv import CSVParser

__all__ = [
    "CSVParser",
]


def load_parser(name: str, **kwargs) -> Parser:
    klass = find_plugin(Path(__file__).parent, __name__, "parser", name)
    return klass(**kwargs)
