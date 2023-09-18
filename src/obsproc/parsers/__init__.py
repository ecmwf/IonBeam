# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #
from pathlib import Path


from ..core.plugins import find_plugin
from ..core.bases import Parser

from .csv import CSVParser
from .csv_file_chunker import CSVChunker

__all__ = [
    "CSVParser",
]


def load_parser(name: str, **kwargs) -> Parser:
    klass = find_plugin(Path(__file__).parent, __name__, "parser", name)
    return klass(**kwargs)
