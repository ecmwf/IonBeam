from functools import cache
import dataclasses
from dataclasses import dataclass, field, fields, asdict, is_dataclass
from typing import Union, get_origin, get_args, List
import typing
from pathlib import Path

# TYPE_KEY is a special key that determines which class or type to pick in ambiguous cases.
# Case 1: Unions: like "str | int"
# Case 2: is the type specified is a class and name is given, the subclasses will be searched for one that matches.
TYPE_KEY = "class"
LINE_KEY = "__line__"  # special key that tells us what line we're on


class ConfigError(Exception):
    "Generic error from the config parser."
    pass


class ConfigMatchError(ConfigError):
    "Raised when the yaml data keys do not match the specification from the python dataclasses."
    pass


class ConfigLineError(ConfigError):
    def __init__(self, context, key, _type, value, message):
        line_number = value.get(LINE_KEY, "?") if hasattr(value, "get") else "?"
        message = f"""{context.filepath}:{line_number}
        While parsing key '{key}' with type '{_type}' and value {value} encountered an error:
        {message}"""
        super().__init__(message)


def has_default(field):
    "Determine whether a dataclass field has a default value"
    return (field.default is not dataclasses.MISSING) or (field.default_factory is not dataclasses.MISSING)


def check_matching(context, datacls, input_dict):
    """Checks if the given dataclass and input contain the same keys,
    otherwise raises and informative error message"""
    # optional_keys = {field.name for field in fields(datacls) if is_optional(field.type)}
    default_keys = {field.name for field in fields(datacls) if has_default(field)}

    ignored_keys = {TYPE_KEY, LINE_KEY} | default_keys
    datacls_keys = {field.name for field in fields(datacls)}
    input_keys = set(input_dict.keys())

    if set.symmetric_difference(input_keys, datacls_keys) - ignored_keys:
        raise ConfigMatchError(
            f"""Invalid config yaml entry
        {context.filepath}:{input_dict.get(LINE_KEY, '?')}
        Determined the type to be '{datacls.__name__}', check this is right.
        Extra keys: {input_keys - datacls_keys - ignored_keys}
        Missing keys: {datacls_keys - input_keys - ignored_keys}
        Default keys (for reference): {default_keys}

        Values in the entry:
        {input_dict}
        """
        )


@dataclass
class Subclasses:
    """Get all the subclasses for given class
    returns {name : class}
    deduplicated by name
    cached
    """

    cache: dict = field(default_factory=dict)

    def get(self, target):
        try:
            return self.cache[target.__name__]
        except KeyError:
            # Deduplicate classes by __name__
            deduped = list({subcls.__name__: subcls for subcls in target.__subclasses__()}.values())
            subclasses = {target.__name__: target} | {k: v for subcls in deduped for k, v in self.get(subcls).items()}
            self.cache[target.__name__] = subclasses
            return subclasses


def is_union(t):
    """Determine whether a type is a Union,
    Implementation detail: str | int and Union[str, int] have types
    Union and UnionType respectively, this works for both.
    """
    # must work on both type | type and Union[type, type]
    UnionType = type(str | None)
    return get_origin(t) in [Union, UnionType]


def is_optional(t):
    """Determine whether a type is optional,
    i.e is it a union that includes None"""
    return is_union(t) and type(None) in get_args(t)


def is_list(t):
    return get_origin(t) == list or t == list


def determine_matching_dataclass(context, key, datacls, input_dict):
    "Check if we actaully want to use a subclass of datacls"
    if not isinstance(input_dict, dict):
        raise ConfigLineError(
            context,
            key,
            datacls,
            {},
            f"Config yaml entry for section '{datacls.__name__}' invalid"
            f" got {input_dict} but was expecting a dictionary entry",
        )

    if TYPE_KEY in input_dict:
        subclasses = context.subclasses.get(datacls)
        cls_name = input_dict[TYPE_KEY]
        # print(subclasses, cls_name)
        try:
            return subclasses[cls_name]
        except KeyError:
            raise ConfigLineError(
                context,
                key,
                datacls,
                input_dict,
                f"Config yaml entry for section '{datacls.__name__}' invalid"
                f" name: '{cls_name}' could not be found as a subclass of {datacls.__name__}"
                f"\n\nKnown subclasses: {subclasses.keys()}",
            ) from None

    return datacls


def parse_list(context, key, list_type, value):
    args = get_args(list_type)
    if list_type == list or not args:
        raise ConfigLineError(
            context,
            key,
            list_type,
            value,
            f"Cannot use bare type '{list_type.__name__}',"
            f"you must specifiy what type it contains i.e {list_type.__name__}[int].",
        ) from None

    contained_type = args[0]
    return [parse_field(context, f"element {i}", contained_type, v) for i, v in enumerate(value)]


def is_literal(t):
    return get_origin(t) is typing.Literal


def parse_literal(context, key, _type, value):
    literal_values = get_args(_type)
    assert value in literal_values
    return value


def parse_union(context, key, _type, value):
    if is_optional(_type) and value is None:
        return None

    # strip off the None from the list of types
    types = [t for t in get_args(_type) if t is not type(None)]

    if len(types) == 1:
        _type = types[0]

    elif len(types) > 1:
        if not all(is_dataclass(t) for t in types):
            raise ConfigLineError(
                context,
                key,
                _type,
                value,
                f"Union types of multiple non-dataclasses are not allowed"
                " because there's no way to decide which one to use for parsing!",
            )

        if not isinstance(value, dict) or TYPE_KEY not in value:
            raise ConfigLineError(
                context,
                key,
                _type,
                value,
                f"To use unions of types the input data must have a '{TYPE_KEY}' field specifying which one to use",
            )

        _type = next(t for t in types if t.__name__ == value[TYPE_KEY])

    return parse_field(context, key, _type, value)


def parse_field(context, key, _type, value):
    """Given a data class Field object or a bare type and a value object, try to parse it
    Strategy:
    1) if it's a dataclass use dataclass_from_dict
    2) if it's registered as a custom type, use that
    3) Otherwise, we hope it's something like like (str, int, Path...) where you
    can just call the type to construct the object from a string.

    """
    # print(_type, value)
    if is_dataclass(_type):
        # possibly use a subclass instead of the base class
        datacls = determine_matching_dataclass(context, key, _type, value)
        # print(key, _type, value, datacls)
        return dataclass_from_dict(context, datacls, value)

    if is_union(_type):
        return parse_union(context, key, _type, value)

    if is_list(_type):
        return parse_list(context, key, _type, value)

    if is_literal(_type):
        return parse_literal(context, key, _type, value)

    try:
        result = _type(value)
    except Exception as e:
        raise ConfigLineError(context, key, _type, value, str(e)) from None

    return result


@dataclass
class Context:
    filepath: Path | str
    subclasses: Subclasses = field(default_factory=Subclasses)


def dataclass_from_dict(context, datacls, input_dict):
    "Given a JSON/YAML like dict of nested dicts/lists, parse it to datacls"
    check_matching(context, datacls, input_dict)

    kwargs = {
        field.name: parse_field(context, field.name, field.type, input_dict[field.name])
        for field in fields(datacls)
        if field.name in input_dict
    }

    return datacls(**kwargs)


def parse_config_from_dict(datacls, input_dict, filepath: Path | str | None = None):
    context = Context(filepath or "???")
    return dataclass_from_dict(context, datacls, input_dict)
