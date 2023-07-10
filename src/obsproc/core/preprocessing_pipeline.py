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


# If this list gets long it could be automated
from ..parsers.csv import CSVParser

Parser = CSVParser  # | XLSXParser

logger = logging.getLogger(__name__)


# @dataclasses.dataclass
# class Preprocessor(LoadMixin, Processor):
#     """
#     A pipeline for processing incoming data

#     :param parser: A configuration dictionary describing the parser to construct
#     :param name: (Optional) the name of the pipeline to construct
#     :param metadata: Additional metadata to annotate all of the data for this dataset
#     :param match: A regex to identify matching data on the supplied id. Named groups identify metadata
#     """

#     match: Pattern
#     parser: Parser
#     metadata: None | Dict[str, str] = dataclasses.field(default_factory=dict)
#     name: str | None = "<unnamed>"

#     def __post_init__(self):
#         self.compiled_match = re.compile(self.match)
#         self.name = self.parser.name

#     def matches(self, rawdata: FileMessage):
#         return self.compiled_match.match(rawdata.id)

#     def process(self, rawdata: FileMessage | FinishMessage) -> Iterable[TabularMessage]:
#         if isinstance(rawdata, FinishMessage):
#             return []
#         m = self.compiled_match.match(rawdata.id)
#         assert m

#         parsed_data = self.parser.parse(rawdata)

#         return parsed_data

#     def __repr__(self):
#         return f"{self.__class__.__name__}({self.match.pattern})"

#     @staticmethod
#     def load_to_pattern(o: AnyStr, *_) -> re.Pattern:
#         return re.compile(o)


# Preprocessor.register_load_hook(re.Pattern, Preprocessor.load_to_pattern)
