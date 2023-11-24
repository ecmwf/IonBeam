from pathlib import Path

import pe
from pe.actions import Constant, Pack, Pair, Fail, Capture, Bind, Call
from pe.operators import Class, Star

import dataclasses
from dataclasses import dataclass, field, fields
from typing import Any


@dataclass(frozen=True)
class Attr:
    key: str
    type: str | None = None
    flag: str | None = None
    values: tuple = field(default_factory=tuple)
    comment: str = ""

    def __repr__(self):
        exclude = set(["key", "comment"])
        kv = {f.name: val for f in fields(self) if not f.name in exclude and (val := getattr(self, f.name))}
        args = ",".join(f"{v}" for k, v in kv.items())
        if args:
            args = f"({args})"
        return f"{self.key}{args}"

    def matches(self, key, value):
        if self.key != key:
            return False
        if self.values:
            if not value in self.values:
                return False
        return True

    def is_optional(self):
        if self.flag is None:
            return False
        return "?" in self.flag


@dataclass(frozen=True)
class Comment:
    value: str


@dataclass(frozen=True)
class TypeDef:
    key: str
    type: str


fdb_schema = pe.compile(
    r"""
    FDB < Line+ EOF
    Line < Schema / Comment / TypeDef / empty

    # Comments
    Comment <- "#" ~non_eol*
    non_eol              <- [\x09\x20-\x7F] / non_ascii
    non_ascii            <- [\x80-\uD7FF\uE000-\U0010FFFF]

    # Deafault Type Definitions
    TypeDef < String ":" String ";"

    # Schemas are the main attraction
    # They're really a list of Attributes
    Schema < "[" Attributes (","? Schema)* "]"

    # Attributes can be just a name i.e expver
    # Can also have a type expver:int
    # Or a flag expver?
    # Or values expver=xxx
    Attributes < Attr_ws ("," Attr_ws)*
    Attr_ws < Attr
    Attr <- key:String (flag:Flag)? (type:Type)? (values:Values)? ([ ]* comment:Comment)?
    Flag <- ~("?" / "-")
    Type <- ":" [ ]* String
    Values <- "=" String ("/" String)*

    # Low level stuff 
    String   <- ~([a-zA-Z0-9]+)
    EOF  <- !.
    empty <- ""
    """,
    actions={
        "Schema": Pack(tuple),
        "Attr": Attr,
        "Values": Pack(tuple),
        "Comment": Comment,
        "TypeDef": TypeDef,
    },
    ignore=Star(Class("\t\f\r\n ")),
    # flags=pe.DEBUG,
)


def post_process(entries):
    typedefs = {}
    schemas = []
    for entry in entries:
        match entry:
            case c if isinstance(c, Comment):
                pass
            case t if isinstance(t, TypeDef):
                typedefs[t.key] = t.type
            case s if isinstance(s, tuple):
                schemas.append(s)
            case _:
                raise ValueError
    return typedefs, tuple(schemas)


def determine_types(types, node):
    if isinstance(node, tuple):
        return [determine_types(types, n) for n in node]

    if not node.type:
        type = types.get(node.key, None)
        return dataclasses.replace(node, type=type)

    return node


def parse_schemas(string):
    m = fdb_schema.match(string)
    g = list(m.groups())
    types, schemas = post_process(g)
    schemas = determine_types(types, schemas)
    return schemas


def flatten(iterable):
    it = iter(iterable)
    for e in it:
        if isinstance(e, (list, tuple)):
            for f in flatten(e):
                yield f
        else:
            yield e


@dataclass
class KeyMatch:
    key: str
    value: Any
    key_spec: Attr
    reason: str

    def good(self):
        return self.reason in {"Matches", "Skipped"}

    def info(self):
        return f"{'✅' if self.good() else '❌'} {self.key:<12}= {self.value:<12} ({self.key_spec}) {self.reason if not self.good() else ''}"


class FDBSchema:
    def __init__(self, string):
        self.schema = parse_schemas(string)[0]

        if isinstance(self.schema[-2], list) or isinstance(self.schema[-1][-2], list):
            raise ValueError("Schemas with multiple second and third level paths are not yet supported.")

        # TODO Support having multiple sublevels
        self.flat_schema = list(flatten(self.schema))

    def __repr__(self):
        return repr(self.schema)

    def match(self, request: dict[str, Any]):
        for key_spec in self.flat_schema:
            key = key_spec.key
            try:
                value = request[key]
            except KeyError:
                if key_spec.is_optional():
                    yield KeyMatch(key_spec.key, "", key_spec, "Skipped")
                else:
                    yield KeyMatch(key_spec.key, "", key_spec, "Key Missing")
                continue

            if key_spec.matches(key, value):
                yield KeyMatch(key_spec.key, value, key_spec, "Matches")
            else:
                yield KeyMatch(key_spec.key, value, key_spec, "Incorrect Value")
