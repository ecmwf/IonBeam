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
from typing import Iterable, Callable, List
import re
import pandas
from pathlib import Path


@dataclasses.dataclass
class MetaData:
    source: str
    observation_variable: None | str = None
    time_slice: pandas.Period | None = None
    filepath: Path | None = None
    unstructured: dict = dataclasses.field(kw_only=True, default_factory=dict)

    def __str__(self):
        return f"{self.__class__.__name__}(source = {self.source}, variable = {self.observation_variable})"


@dataclasses.dataclass
class Message:
    "Base Message Class from which FinishMessage and DataMessage inherit"
    pass


@dataclasses.dataclass
class FinishMessage(Message):
    "Indicates no more messages will come down the pipeline."
    reason: str


@dataclasses.dataclass
class DataMessage(Message):
    "A message that represents some data. It may have an attached dataframe, a reference to a file or somethihg else."
    metadata: MetaData

    def __str__(self):
        class_name = self.__class__.__name__
        arg_string = []
        for name, key in {"src": "source", "obs": "observation_variable", "timeslc": "time_slice"}.items():
            val = getattr(self.metadata, key, None)
            if val is not None:
                arg_string.append(f"{name} = {val}")

        return f"{class_name}({', '.join(arg_string)})"


@dataclasses.dataclass
class FileMessage(DataMessage):
    pass


@dataclasses.dataclass
class TabularMessage(DataMessage):
    data: pandas.DataFrame


MessageStream = Iterable[Message]
GenerateFunction = Callable[[], Iterable[Message]]
ProcessFunction = Callable[[Message], Iterable[Message]]


@dataclasses.dataclass
class Action:
    "The base class for actions, from which Source and Processor inherit"

    def resolve_path(self, path: str | Path) -> Path:
        base = Path(__file__).parents[3]
        path = Path(path)
        if not path.is_absolute():
            path = base / path
        return path


@dataclasses.dataclass
class Source(Action):
    _generator = None

    def __iter__(self):
        return self

    def __next__(self):
        if self._generator is None:
            self._generator = iter(self.generate())
        assert self._generator is not None
        return next(self._generator)

    def generate(self) -> Iterable[Message]:
        raise NotImplementedError


@dataclasses.dataclass
class Match:
    """
    Represents a filter on incoming messages. A message is considered to match a Match object if
    all of the corresponding keys match. Any key on the Match object that is None is ignored.
    Match fields may be strings or regex.
    """

    filepath: str | None = None
    "The filepath to the data"
    source: str | None = None
    "The logical data source"

    observation_variable: str | None = None
    "The observation variable, i.e air_temperature_near_surface,"
    "see the list of canonical variables"

    def matches(self, msg: DataMessage) -> bool:
        "Determine if msg matches this Match object"
        for field in dataclasses.fields(self):
            match_regex = getattr(self, field.name)
            if match_regex is None:
                continue
            msg_string = str(getattr(msg.metadata, field.name))
            if not re.match(match_regex, msg_string):
                return False
        return True

    def __repr__(self):
        class_name = self.__class__.__name__
        arg_string = []
        for key, field in self.__dataclass_fields__.items():
            val = getattr(self, key, None)
            if val is not None:
                arg_string.append(f"{key} = '{val}'")

        return f"{class_name}({', '.join(arg_string)})"


@dataclasses.dataclass
class Processor(Action):
    match: List[Match]

    def matches(self, message: Message) -> bool:
        if isinstance(message, FinishMessage):
            return True
        assert isinstance(message, DataMessage)
        return any(matcher.matches(message) for matcher in self.match)

    def process(self, msg: Message) -> Iterable[Message]:
        raise NotImplementedError


class Parser(Processor):
    pass


class Aggregator(Processor):
    pass


# Config Classes
@dataclasses.dataclass
class CanonicalVariable:
    """
    Represents a physical variable with units, dtype and other metadata
    name: our internal name, i.e air_temperature_near_surface
    desc: a description of the variable
    unit: the physical unit
    WMO: whether this exact name exists in the WMO variables
    dtype: the dtype with which to store this data internally
    output: whether this variable should be output as an observation at the end of the pipeline

    These three set the MARS keys for this variable
    codetype: int
    varno: int
    obstype: int
    """

    name: str
    desc: str | None = None
    unit: str | None = None
    WMO: bool = False
    dtype: str | None = "float64"
    output: bool = False

    codetype: int | None = None
    varno: int | None = None
    obstype: int | None = None


@dataclasses.dataclass
class InputColumn:
    """Represents a variable within an external stream of data
    and links it to our nice internal representation.

    name: our internal name, i.e air_temperature_near_surface
    key: the column name within the incoming data stream perhaps T0, temp, temperatura
    type: if not None, gives a hint to the parser of how to parse this column
    unit: the incoming physical unit, i.e degrees celsius
    canonical_variable: linked at runtime to the canonical_variable that this column will get parsed to.
    """

    name: str
    key: str = "__DEFAULT_TO_NAME__"
    type: type | str | None = None
    unit: str | None = None
    canonical_variable: CanonicalVariable | None = None

    def __post_init__(self):
        if self.key == "__DEFAULT_TO_NAME__":
            self.key = self.name
