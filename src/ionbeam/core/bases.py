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
from typing import Iterable, Callable, List, Literal, TypeVar, Any
import re
import pandas
from pathlib import Path
import os
from .html_formatters import dataclass_to_html, message_to_html
from .history import (
    PreviousActionInfo,
    ActionInfo,
    MessageInfo,
    CodeSourceInfo,
    describe_code_source,
)


@dataclasses.dataclass(unsafe_hash=True)
class MetaData:
    state: str | None = None
    source: None | str = None
    observation_variable: None | str = None
    time_slice: pandas.Period | None = None
    encoded_format: str | None = None
    filepath: Path | None = None
    # unstructured: dict = dataclasses.field(kw_only=True, default_factory=dict)

    def __str__(self):
        return f"{self.__class__.__name__}(source = {self.source}, variable = {self.observation_variable})"

    def _repr_html_(self):
        return dataclass_to_html(self)


@dataclasses.dataclass
class Message:
    "Base Message Class from which FinishMessage and DataMessage inherit"

    def _repr_html_(self):
        return message_to_html(self)


@dataclasses.dataclass
class FinishMessage(Message):
    "Indicates no more messages will come down the pipeline."
    reason: str


@dataclasses.dataclass
class DataMessage(Message):
    "A message that represents some data. It may have an attached dataframe, a reference to a file or somethihg else."
    metadata: MetaData
    history: list = dataclasses.field(default_factory=list, kw_only=True)

    def __str__(self):
        class_name = self.__class__.__name__
        arg_string = []
        for name, key in {
            "state": "state",
            "src": "source",
            "obs": "observation_variable",
            "timeslc": "time_slice",
        }.items():
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
    columns: list = dataclasses.field(default_factory=list, kw_only=True)


MessageStream = Iterable[Message]
GenerateFunction = Callable[[], Iterable[Message]]
ProcessFunction = Callable[[Message], Iterable[Message]]

# used to indicate that a function accepts a subclass of DataMessage and returns that same subclass
DataMessageVar = TypeVar("DataMessageVar", bound=DataMessage)


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
    CRS: str | None = None
    WMO: bool = False
    dtype: str | None = "float64"
    output: bool = False

    codetype: int | None = None
    varno: int | None = None
    obstype: int | None = None

    def __repr__(self):
        return f"CanonicalVariable({self.name})"


@dataclasses.dataclass
class Globals:
    canonical_variables: List[CanonicalVariable]
    config_path: Path
    data_path: Path
    online: bool
    code_source: CodeSourceInfo | None = None


@dataclasses.dataclass
class Action:
    "The base class for actions, from which Source and Processor inherit"

    # kw_only is necessary so that classes that inherit from this one can have positional fields
    metadata: MetaData = dataclasses.field(default_factory=MetaData, kw_only=True)
    # code_source: CodeSourceInfo | None = dataclasses.field(default=None, kw_only=True)
    globals: Globals | None = dataclasses.field(default=None, kw_only=True)

    def init(self, globals):
        "Initialise self with access to the global config variables"
        self.globals = globals

    def resolve_path(self, path: str | Path, type: Literal["data", "config"] = "data") -> Path:
        assert self.globals
        if type == "data":
            base = self.globals.data_path
        elif type == "config":
            base = self.globals.config_path
        else:
            raise ValueError(f"{type} must be 'config' or 'data'")

        path = Path(path)
        if not path.is_absolute():
            path = base / path

        return Path(path)

    def generate_metadata(self, message: DataMessage | None = None, **explicit_keys):
        "Defines the semantics for combining metadata from multiple sources"

        def filter_none(d) -> dict:
            if dataclasses.is_dataclass(d):
                d = dataclasses.asdict(d)
            return {k: v for k, v in d.items() if v is not None}

        message_keys = filter_none(message.metadata) if message else {}
        action_keys = filter_none(self.metadata)

        # sources to the right override keys from earlier sources
        keys = message_keys | action_keys | explicit_keys
        return MetaData(**keys)

    def tag_message(self, msg: DataMessageVar, previous_msg: DataMessage | MessageInfo | None) -> DataMessageVar:
        """
        Update the message history, using the current message and the previous message
        If there is no logical previous message, such as when doing TimeAggregation,
        you can directly pass a MessageInfo object in place of previous_message.
        """
        if isinstance(previous_msg, DataMessage):
            message = MessageInfo(name=previous_msg.__class__.__name__, metadata=previous_msg.metadata)
        elif isinstance(previous_msg, MessageInfo):
            message = previous_msg
        else:
            raise ValueError(f"previous_msg was of type {type(previous_msg)} not DataMessage or MessageInfo")

        msg.history = previous_msg.history.copy() if isinstance(previous_msg, DataMessage) else []
        msg.history.append(
            PreviousActionInfo(
                action=ActionInfo(name=self.__class__.__name__, code=self.globals.code_source),
                message=message,
            )
        )
        return msg


@dataclasses.dataclass
class Source(Action):
    "An Action which only outputs messages."

    def __post_init__(self):
        # Set the dafault value of parsed but let it be overridden
        self.metadata = dataclasses.replace(self.metadata, state="raw")

    def __iter__(self):
        return self

    def __next__(self):
        if not hasattr(self, "_generator"):
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

    state: str | None = None
    "The state of the message"
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
    "An Action which accepts and returns messages"
    match: List[Match]

    def matches(self, message: Message) -> bool:
        if isinstance(message, FinishMessage):
            return True
        return any(matcher.matches(message) for matcher in self.match)

    def process(self, msg: Message) -> Iterable[Message]:
        raise NotImplementedError


@dataclasses.dataclass
class Parser(Processor):
    """
    An action which takes raw input messages and outputs parsed ones.
    Changes message.metadata.state from raw to parsed.

    """

    def init(self, globals):
        super().init(globals)
        # Set the dafault value of parsed but let it be overridden
        self.metadata = dataclasses.replace(self.metadata, state="parsed")


class Aggregator(Processor):
    pass


@dataclasses.dataclass
class EncodedMessage(TabularMessage):
    format: str

    def __str__(self):
        return f"EncodedMessage(format={self.format})"


@dataclasses.dataclass
class Encoder(Processor):
    def encode(self, parsed_data: TabularMessage) -> Iterable[EncodedMessage]:
        raise NotImplementedError("Implement encode() in derived class")

    def process(self, parsed_data: TabularMessage) -> Iterable[EncodedMessage]:
        return self.encode(parsed_data)

    def __post_init__(self):
        self.metadata = dataclasses.replace(self.metadata, state="encoded")


@dataclasses.dataclass
class Writer(Processor):
    def process(self, data: EncodedMessage) -> Iterable[Message]:
        raise NotImplementedError("Implement encode() in derived class")

    def __post_init__(self):
        self.metadata = dataclasses.replace(self.metadata, state="written")


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
    discard: bool = False
    canonical_variable: CanonicalVariable | None = None

    def __post_init__(self):
        if self.key == "__DEFAULT_TO_NAME__":
            self.key = self.name
