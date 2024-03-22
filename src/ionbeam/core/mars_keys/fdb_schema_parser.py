from pathlib import Path

import pe
from pe.actions import Constant, Pack, Pair, Fail, Capture, Bind, Call
from pe.operators import Class, Star
import re
import pandas as pd
from ...core.html_formatters import dataframe_to_html

import dataclasses
from dataclasses import dataclass, field, fields
from typing import Any, Callable, final
from io import StringIO
from collections import defaultdict
from datetime import datetime
import json
import pyodc

from .fdb_types import FDBType, FDB_type_to_implementation


@dataclass(frozen=True)
class KeySpec:
    """
    Represents the specification of a single key in an FDB schema file. For example in
    ```
    [ class, expver, stream=lwda, date, time, domain?
       [ type=ofb/mfb/oai
               [ obsgroup, reportype ]]]
    ```
    class, expver, type=ofdb/mfb/oai etc are the KeySpecs

    These can have additional information such as: flags like `domain?`, allowed values like `type=ofb/mfb/oai`
    or specify type information with `date: ClimateMonthly`

    """

    key: str
    type: FDBType = field(default_factory=FDBType)
    flag: str | None = None
    values: tuple = field(default_factory=tuple)
    comment: str = ""

    def __repr__(self):
        repr = self.key
        if self.flag:
            repr += self.flag
        # if self.type:
        #     repr += f":{self.type}"
        if self.values:
            repr += "=" + "/".join(self.values)
        return repr

    def matches(self, key, value):
        # Sanity check!
        if self.key != key:
            return False

        # Some keys have a set of allowed values type=ofb/mfb/oai
        if self.values:
            if not value in self.values:
                return False

        # Check the formatting of values like Time or Date
        if self.type and not self.type.validate(value):
            return False

        return True

    def is_optional(self):
        if self.flag is None:
            return False
        return "?" in self.flag


@dataclass(frozen=True)
class Comment:
    "Represents a comment node in the schema"
    value: str


@dataclass(frozen=True)
class FDBSchemaTypeDef:
    "Mapping between FDB schema key names and FDB Schema Types, i.e expver is of type Expver"
    key: str
    type: str


# This is the schema grammar written in PEG format
fdb_schema = pe.compile(
    r"""
    FDB < Line+ EOF
    Line < Schema / Comment / TypeDef / empty

    # Comments
    Comment <- "#" ~non_eol*
    non_eol              <- [\x09\x20-\x7F] / non_ascii
    non_ascii            <- [\x80-\uD7FF\uE000-\U0010FFFF]

    # Default Type Definitions
    TypeDef < String ":" String ";"

    # Schemas are the main attraction
    # They're a tree of KeySpecs.
    Schema < "[" KeySpecs (","? Schema)* "]"

    # KeySpecs can be just a name i.e expver
    # Can also have a type expver:int
    # Or a flag expver?
    # Or values expver=xxx
    KeySpecs < KeySpec_ws ("," KeySpec_ws)*
    KeySpec_ws < KeySpec
    KeySpec <- key:String (flag:Flag)? (type:Type)? (values:Values)? ([ ]* comment:Comment)?
    Flag <- ~("?" / "-")
    Type <- ":" [ ]* String
    Values <- "=" String ("/" String)*

    # Low level stuff 
    String   <- ~([a-zA-Z0-9_]+)
    EOF  <- !.
    empty <- ""
    """,
    actions={
        "Schema": Pack(tuple),
        "KeySpec": KeySpec,
        "Values": Pack(tuple),
        "Comment": Comment,
        "TypeDef": FDBSchemaTypeDef,
    },
    ignore=Star(Class("\t\f\r\n ")),
    # flags=pe.DEBUG,
)


def post_process(entries):
    "Take the raw output from the PEG parser and split it into type definitions and schema entries."
    typedefs = {}
    schemas = []
    for entry in entries:
        match entry:
            case c if isinstance(c, Comment):
                pass
            case t if isinstance(t, FDBSchemaTypeDef):
                typedefs[t.key] = t.type
            case s if isinstance(s, tuple):
                schemas.append(s)
            case _:
                raise ValueError
    return typedefs, tuple(schemas)


def determine_types(types, node):
    "Recursively walk a schema tree and insert the type information."
    if isinstance(node, tuple):
        return [determine_types(types, n) for n in node]
    return dataclasses.replace(node, type=types.get(node.key, FDBType()))


@dataclass
class Key:
    key: str
    value: Any
    key_spec: KeySpec
    reason: str
    odb_table: str | None = None

    def __bool__(self):
        return self.reason in {"Matches", "Skipped"}

    def info(self):
        return f"{'✅' if self else '❌'} {self.key:<12}= {str(self.value):<12} ({self.key_spec}) {self.reason if not self else ''}"

    def as_string(self):
        return self.key_spec.type.format(self.value)
    
    def as_json(self):
        return dict(
            key = self.key,
            value = self.as_string(),
            reason = self.reason,
        )


class MARSRequest(dict):
    def _repr_html_(self):
        df = pd.DataFrame.from_records(
            dict(
                Matches="✅" if k else "❌",
                Key=k.key,
                Value=k.value,
                Key_spec=k.key_spec,
                Reason=k.reason if k.reason != "Matches" else "",
                Type=k.key_spec.type,
            )
            for k in self.values()
        )
        return dataframe_to_html(df)

    def __bool__(self):
        return all(self.values())

    def as_dict(self):
        return {k: v.value for k, v in self.items()}

    def as_strings(self):
        return {k: v.as_string() for k, v in self.items()}
    
    def as_json(self):
        return [k.as_json() for k, v in self.items()]


class FDBSchema:
    """
    Represents a parsed FDB Schema file.
    Has methods to validate and convert request dictionaries to a mars request form with validation and type information.
    """

    def __init__(self, string):
        """
        1. Use a PEG parser on a schema string,
        2. Separate the output into schemas and typedefs
        3. Insert any concrete implementations of types from fdb_types.py defaulting to generic string type
        4. Walk the schema tree and annotate it with type information.
        """
        m = fdb_schema.match(string)
        g = list(m.groups())
        types, schemas = post_process(g)
        types = {key: FDB_type_to_implementation[type] for key, type in types.items()}
        self.schemas = determine_types(types, schemas)

    def __repr__(self):
        return json.dumps(self.schemas, indent=4, default=repr)

    def strip_ODB_table_keys(self, request: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
        "Filter out at ODB keys of the form 'key@table' from the request and return those separately"
        _request, odb_tables = {}, {}
        for k, v in request.items():
            if "@" in k:
                key, odb_table = k.split("@")
            else:
                key, odb_table = k, None
            _request[key] = v
            odb_tables[key] = odb_table
        return _request, odb_tables
    
    def split_request(self, request: dict[str, Any]) -> tuple[dict, MARSRequest]:
        """Use the schema to split a request into parts in the schema 
        and keys values pairs like filter that are really arguments
        
        returns arguments, mars_request
        """
        _, best_match = self.matches(request)
        keys = set(k.key for k in best_match)
        arguments = {k : v for k,v in request.items() if k not in keys}
        return arguments, MARSRequest({key.key: key for key in best_match})

    @classmethod
    def consume_key(cls, key_spec: KeySpec, request: dict[str, Any], odb_tables: dict[str, str] = {}) -> Key:
        key = key_spec.key
        try:
            value = request[key]
        except KeyError:
            if key_spec.is_optional():
                return Key(key_spec.key, "", key_spec, "Skipped", None)
            else:
                return Key(key_spec.key, "", key_spec, "Key Missing", odb_tables.get(key))

        if key_spec.matches(key, value):
            return Key(key_spec.key, key_spec.type.parse(value), key_spec, "Matches", odb_tables.get(key))
        else:
            return Key(key_spec.key, value, key_spec, "Incorrect Value", odb_tables.get(key))

    @classmethod
    def _DFS_match(cls, tree: list, request: dict[str, Any]) -> tuple[bool | list, list[Key]]:
        """Do a DFS on the schema tree, returning the deepest matching path
        At each stage return whether we matched on this path, and the path itself.

        When traversing the tree there are three cases to consider:
        1. base case []
        2. one schema [k, k, k, [k, k, k]]
        3. list of schemas [[k,k,k], [k,k,k], [k,k,k]]
        """
        #  Case 1: Base Case
        if not tree:
            return True, []

        # Case 2: [k, k, k, [k, k, k]]
        if isinstance(tree[0], KeySpec):
            node, *tree = tree
            # Check if this node is in the request
            match_result = cls.consume_key(node, request)

            # If if isn't then terminate this path here
            if not match_result:
                return False, [match_result,]  # fmt: skip

            # Otherwise continue walking the tree and return the best result
            matched, path = cls._DFS_match(tree, request)

            # Don't put the key in the path if it's optional and we're skipping it.
            if match_result.reason != "Skipped":
                path = [match_result,] + path  # fmt: skip

            return matched, path

        # Case 3: [[k, k, k], [k, k, k]]
        branches = []
        for branch in tree:
            matched, branch_path = cls._DFS_match(branch, request)

            # If this branch matches, terminate the DFS and use this.
            if matched:
                return branch, branch_path
            else:
                branches.append(branch_path)

        # If no branch matches, return the one with the deepest match
        return False, max(branches, key=len)
    
    def matches(self, request: dict[str, Any]):
        request, odb_tables = self.strip_ODB_table_keys(request)
        return self._DFS_match(self.schemas, request)

    def parse(self, request: dict[str, Any]) -> tuple[MARSRequest, list]:
        request, odb_tables = self.strip_ODB_table_keys(request)
        schema_branch, path = self._DFS_match(self.schemas, request)

        if not schema_branch:
            raise ValueError("Given request does not match any schema!\n" + "\n".join(k.info() for k in path))

        return MARSRequest({key.key: key for key in path}), schema_branch
    
    def request_from_odb(self, fileorbuffer):
        df = pyodc.read_odb(fileorbuffer, single = True)
        request = {key : value for key, value in zip(df.columns, df.iloc[0])}
        match, branch = self.parse(request)
        return match, branch


class FDBSchemaFile(FDBSchema):
    def __init__(self, path: str):
        with open(path, "r") as f:
            return super().__init__(f.read())
