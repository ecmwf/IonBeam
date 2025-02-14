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
import itertools as it
import re
import uuid
from copy import deepcopy
from dataclasses import dataclass, field, fields
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated, Callable, Iterable, List, Literal, TypeVar
from unicodedata import normalize
from uuid import UUID

import pandas as pd
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import Session

from ..core.converters import unit_conversions
from .history import (
    ActionInfo,
    CodeSourceInfo,
    MessageInfo,
    PreviousActionInfo,
)
from .html_formatters import action_to_html, dataclass_to_html, message_to_html
from .mars_keys import FDBSchema, MARSRequest
from .time import TimeSpan

# from functools import partial
# from pydantic.dataclasses import dataclass

# class Config:
#     arbitrary_types_allowed = True

# dataclass = partial(dataclass, config=Config)



@dataclass(unsafe_hash=True)
class MetaData:
    source_action_id: uuid.UUID | None = None
    state: str | None = None
    source: None | str = None
    observation_variable: None | str = None
    time_span: TimeSpan | None = None
    encoded_format: str | None = None
    filepath: Path | None = None
    mars_id: MARSRequest | None = None
    unstructured: dict = field(kw_only=True, default_factory=dict)

    columns: dict[str, "CanonicalVariable | RawVariable"] = field(default_factory=dict, kw_only=True)

    internal_id: str | None = None
    external_id: str | None = None

    date: str | None = None
    time: str | None = None

    def __str__(self):
        return f"{self.__class__.__name__}(source = {self.source}, variable = {self.observation_variable})"

    def _repr_html_(self):
        return dataclass_to_html(self)


@dataclass
class Message:
    "Base Message Class from which FinishMessage and DataMessage inherit"
    next: List[UUID] | None = field(default=None, kw_only=True)

    def _repr_html_(self):
        return message_to_html(self)
    
    def find_matches(self, actions : dict[UUID, "Processor"]) -> Iterable["Action"]:
        "Find all the actions that match this message"
        
        # Shortcut for simply connecting to the previous action
        if self.next is not None:
            for a_id in self.next:
                yield actions[a_id]
            return 
            
        for a in actions.values():
            if a.matches(self):
                yield a

@dataclass
class FinishMessage(Message):
    "Indicates no more messages will come down the pipeline."

    reason: str


@dataclass
class DataMessage(Message):
    "A message that represents some data. It may have an attached dataframe, a reference to a file or somethihg else."

    metadata: MetaData
    history: list = field(default_factory=list, kw_only=True)

    def __str__(self):
        class_name = self.__class__.__name__
        arg_string = []
        for name, key in {
            "state": "state",
            "src": "source",
            # "obs": "observation_variable",
            # "timeslc": "time_span",
        }.items():
            val = getattr(self.metadata, key, None)
            if val is not None:
                arg_string.append(f"{val}")

        return f"{class_name}({', '.join(arg_string)})"

@dataclass
class TabularMessage(DataMessage):
    data: pd.DataFrame


@dataclass
class FileMessage(DataMessage):
    filepath: Path
    def data_bytes(self):
        return self.filepath.read_bytes()

@dataclass
class BytesMessage(DataMessage):
    bytes: bytes
    def data_bytes(self):
        return self.bytes

MessageStream = Iterable[Message]
GenerateFunction = Callable[[], Iterable[Message]]
ProcessFunction = Callable[[Message], Iterable[Message]]

# used to indicate that a function accepts a subclass of X and returns that same subclass
MessageVar = TypeVar("MessageVar", bound=Message)
DataMessageVar = TypeVar("DataMessageVar", bound=DataMessage)


@dataclass
class Variable:
    name: str
    description: str | None = None
    unit: str | None = None
    type: str | None = None
    CRS: str | None = None
    dtype: str | None = "float64"
    discard: bool = False


@dataclass
class CanonicalVariable(Variable):
    """
    Represents a physical variable with units, dtype and other metadata
    """

    raw_variable: "RawVariable | None" = None

    def __repr__(self):
        return f"CanonicalVariable(name={self.name!r}, unit={self.unit!r}, desc={self.description!r})"

    def __post_init__(self):
        # Make a best effort to deal with unicode characters that look the same but have different code points
        if self.unit:
            self.unit = normalize("NFKD", self.unit)

    # for compatability with RawVariables, CanonicalVariables have the same key as name
    @property
    def key(self):
        return self.name

@dataclass
class RawVariable(Variable):
    """Represents a variable within an external stream of data
    and links it to our nice internal representation.

    name: our internal name, i.e air_temperature_near_surface
    key: the column name within the incoming data stream perhaps T0, temp, temperatura
    type: if not None, gives a hint to the parser of how to parse this column
    unit: the incoming physical unit, i.e degrees celsius
    canonical_variable: linked at runtime to the canonical_variable that this column will get parsed to.
    """

    key: str = "__DEFAULT_TO_NAME__"
    canonical_variable: CanonicalVariable | None = None
    converter: Callable[[pd.Series], pd.Series] | None = None
    metadata: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.key == "__DEFAULT_TO_NAME__":
            self.key = self.name

        # Make a best effort to deal with unicode characters that look the same but have different code points
        if self.unit:
            self.unit = normalize("NFKD", str(self.unit))

    def make_canonical(self) -> CanonicalVariable:
        assert self.canonical_variable is not None, f"{self} has no canonical variable!"
        return dataclasses.replace(
            self.canonical_variable,
            raw_variable=self,
        )


@dataclass
class IngestionTimeConstants:
    # The time span of the data to try to download
    query_timespan: Annotated[TimeSpan, "custom_init"]

    # What size chunks to use for the emitted messages
    granularity: timedelta


@dataclass
class Globals:
    canonical_variables: List[CanonicalVariable]
    canonical_variables_by_name: dict[str, CanonicalVariable] = field(init=False)
    
    data_path: Path
    cache_path: Path
    ingestion_time_constants: IngestionTimeConstants
    source_path: Path | None = field(default=None, kw_only=True)

    die_on_error: bool = False
    
    environment: str = "local"

    metkit_language_template: Path | None = None
    fdb_schema_path: Path | None = None
    fdb_root: Path | None = None

    echo_sql_commands: bool = False

    secrets_file: Path = Path("secrets.yaml")

    download: bool = True
    ingest_to_pipeline : bool = True
    overwrite_fdb: bool = False

    reingest: bool = False
    finish_after: int | None = None

    split_data_columns: bool = True
    code_source: CodeSourceInfo | None = None
    fdb_schema: FDBSchema = field(kw_only=True, init = False)
    secrets: dict | None = None
    api_hostname: str | None = None
    postgres_database: dict | None = None
    custom_mars_keys: list[str] | None = None

    sql_engine: Engine = field(kw_only=True, init=False)
    sql_session: Session = field(kw_only=True, init=False)

    # When a class hasn't been instantiated yet use a string, see https://peps.python.org/pep-0484/#forward-references
    actions_by_id: dict[uuid.UUID, "Action"] = field(default_factory=dict)
    actions_by_name: dict[str, "Action"] = field(default_factory=dict)

    def _repr_html_(self):
        return dataclass_to_html(self)


@dataclass
class Action:
    "The base class for actions, from which Source and Processor inherit"

    # Unique human readable name
    name: str | None = field(default = None, kw_only=True)

    # A list of names of actions to forward messages to next
    forward_to_names: List[str] | None = field(default = None, kw_only=True)

    # Metadata to set on outgoing messages
    set_metadata: MetaData = field(default_factory=MetaData, kw_only=True)

    # code_source: CodeSourceInfo | None = field(default=None, kw_only=True)
    id: uuid.UUID = field(default_factory=uuid.uuid4, kw_only=True)
    next: List[UUID] | None = field(default=None, kw_only=True)
    globals: Globals = field(kw_only=True, init=False)

    "Path to the yaml file that defines this action" 
    definition_path: Path | None = field(kw_only=True, init=False)

    def init(self, globals: Globals, definition_path: Path | None = None):
        "Initialise self with access to the global config variables"
        self.globals = globals
        self.definition_path = definition_path
        self.globals.actions_by_id[self.id] = self
        self.set_metadata.source_action_id = self.id

    def _repr_html_(self):
        return action_to_html(self)

    def __str__(self):
        return f"{self.__class__.__name__}(id = ...{str(self.id)[-5:]})"

    def resolve_path(
        self, path: str | Path, type: Literal["data", "config"] = "data"
    ) -> Path:
        assert self.globals
        if type == "data":
            base = self.globals.data_path
        elif type == "config":
            base = self.globals.source_path
        else:
            raise ValueError(f"{type} must be 'config' or 'data'")

        path = Path(path)
        if not path.is_absolute():
            path = base / path

        return Path(path)

    def generate_metadata(self, message: DataMessage | None = None, **explicit_keys):
        "Defines the semantics for combining metadata from multiple sources"

        def shallow_asdict(d) -> dict:
            return {field.name: getattr(d, field.name) for field in fields(d)}

        def filter_none(d) -> dict:
            if dataclasses.is_dataclass(d):
                d = shallow_asdict(d)
            return {k: v for k, v in d.items() if v is not None and v != {}} 

        message_keys = filter_none(message.metadata) if message else {}
        action_keys = filter_none(self.set_metadata)

        # sources to the right override keys from earlier sources
        keys = message_keys | action_keys | explicit_keys
        return MetaData(**keys)

    def tag_message(
        self, msg: DataMessageVar, previous_msg: DataMessage | MessageInfo | None
    ) -> MessageVar:
        """
        Update the message history, using the current message and the previous message
        If there is no logical previous message, such as when doing TimeAggregation,
        you can directly pass a MessageInfo object in place of previous_message.
        """
        if isinstance(previous_msg, DataMessage):
            message = MessageInfo(
                name=previous_msg.__class__.__name__, metadata=deepcopy(previous_msg.metadata)
            )
        elif isinstance(previous_msg, MessageInfo):
            message = previous_msg
        elif previous_msg is None:
            message = None
        else:
            raise ValueError(f"Unknown previous message type {type(previous_msg)}")
        
        msg.history = (
            previous_msg.history.copy() if isinstance(previous_msg, DataMessage) else []
        )
        msg.history.append(
            PreviousActionInfo(
                action=ActionInfo(
                    name=self.__class__.__name__, code=self.globals.code_source
                ),
                message=message,
            )
        )
        msg.next = self.next
        return msg


@dataclass
class Match:
    """
    Represents a filter on incoming messages. A message is considered to match a Match object if
    all of the corresponding keys match. Any key on the Match object that is None is ignored.
    Match fields may be strings or regex.
    """

    source_action_id: uuid.UUID | None = None
    "only match on messages coming from Actions with this id"

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
        for field in fields(self):
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


@dataclass
class Processor(Action):
    "An Action which accepts and returns messages"

    # UUID is the fast path that just matches instantly with a single previous action
    match: List[Match] | None = field(default=None, kw_only=True)

    def matches(self, message: Message) -> bool:
        if isinstance(message, FinishMessage):
            return True

        assert isinstance(message, DataMessage)
        if self.match is None:
            return False

        # Fast path for just matching an message from a particular action
        if isinstance(self.match, uuid.UUID):
            return message.metadata.source_action_id == self.match

        # Slow path for matching on multiple fields
        return any(matcher.matches(message) for matcher in self.match)

    def process(self, msg: Message) -> Iterable[Message]:
        raise NotImplementedError


@dataclass
class Parser(Processor):
    """
    An action which takes raw input messages and outputs parsed ones.
    Changes message.metadata.state from raw to parsed.
    """

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)


class Aggregator(Processor):
    pass


@dataclass
class EncodedMessage(TabularMessage):
    format: str

    def __str__(self):
        return f"EncodedMessage(format={self.format})"


@dataclass
class Encoder(Processor):
    def encode(self, parsed_data: TabularMessage) -> Iterable[EncodedMessage]:
        raise NotImplementedError("Implement encode() in derived class")

    def process(self, parsed_data: TabularMessage) -> Iterable[EncodedMessage]:
        return self.encode(parsed_data)

    def __post_init__(self):
        self.set_metadata = dataclasses.replace(self.set_metadata, state="encoded")


@dataclass
class Writer(Processor):
    def process(self, data: EncodedMessage) -> Iterable[Message]:
        raise NotImplementedError("Implement encode() in derived class")

    def __post_init__(self):
        self.set_metadata = dataclasses.replace(self.set_metadata, state="written")


class Mappings(list):
    """
    Implement a custom syntax for lists of RawVariables where input columns that look like this:
    ```
    - name: equivalent_carbon_dioxide
    key:
        - eco2
        - eCO2
    unit: ["ppm", "ppb"]
    ```
    will expand to the cartesian product of all the keys and units.
    """

    def __init__(self, iterable):
        # expand mappings to allow for the cartesian product of any key, unit combinations
        def l(d, k, default):
            value = d.get(k, default)
            return value if isinstance(value, list) else [value]

        allowed = {f.name for f in fields(RawVariable)}
        super().__init__(
            [
                RawVariable(
                    **(
                        {k: v for k, v in col.items() if k in allowed}
                        | dict(key=key, unit=unit)
                    )
                )
                for col in iterable
                for key, unit in it.product(
                    l(col, "key", col["name"]), l(col, "unit", None)
                )
            ]
        )

    def link(self, canonical_variables):
        canonical_variables = {c.name: c for c in canonical_variables}
        for col in self:
            if col.discard: continue
            try:
                col.canonical_variable = canonical_variables[col.name]
            except KeyError:
                raise ValueError(f"Could not find canonical variable for {col.name}")

            if (col.unit is not None) and (col.unit != col.canonical_variable.unit):
                try:
                    self.converter = unit_conversions[
                                normalize("NFKD", f"{col.unit.strip()} -> {col.canonical_variable.unit.strip()}")
                            ]
                except KeyError:
                        raise ValueError(
                            f"No unit conversion registered for {col.name}: {col.unit} -> {col.canonical_variable.unit}"
                        )
