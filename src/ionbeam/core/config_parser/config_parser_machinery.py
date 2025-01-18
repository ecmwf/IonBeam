import dataclasses
import typing
from dataclasses import Field, dataclass, field
from pathlib import Path
from typing import Union, get_args, get_origin

import Levenshtein

from .common import ConfigError, ConfigMatchError

# TYPE_KEY is a special key that determines which class or type to pick in ambiguous cases.
# Case 1: Unions: like "str | int"
# Case 2: is the type specified is a class and name is given, the subclasses will be searched for one that matches.
TYPE_KEY = "class"
LINE_KEY = "__line__"  # special key that tells us what line we're on

@dataclass
class MissingOverlay:
    def __bool__(self):
        return False

MISSING_OVERLAY = MissingOverlay()  # special object to indicate that a field is missing in an overlay


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
    default_keys = {field.name for field in fields(datacls) if has_default(field)}

    ignored_keys = {TYPE_KEY, LINE_KEY} | default_keys
    datacls_keys = {field.name for field in fields(datacls) if field.init}
    input_keys = set(input_dict.keys())

    if context.overlay:
        # Only complain about extra keys
        diff = set.difference(input_keys, datacls_keys)
    else:
        # Complain about both missing keys and extra keys, hence symmetric difference
        diff = set.symmetric_difference(input_keys, datacls_keys)

    if diff - ignored_keys:
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
    
def to_bare_types(type_object: type):
  # For a given `X[Y, Z, ...]`:
  # * `get_origin` returns `X`
  # * `get_args` return `(Y, Z, ...)`
  # For non-supported type objects, the return values are
  # `None` and `()` correspondingly.
  origin, args = get_origin(type_object), get_args(type_object)
    
  if origin is None:
    return type_object
    
  if origin is typing.Annotated:
    bare_type = get_args(type_object)[0]
    return to_bare_types(bare_type)

  return origin

# Custom versions of dataclassses.is_dataclass and dataclasses.fields that handle Annotated dataclass types
def is_dataclass(t):
    return dataclasses.is_dataclass(to_bare_types(t))

def fields(t):
    return dataclasses.fields(to_bare_types(t))



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


def is_dict(t):
    return get_origin(t) == dict or t == dict

def is_overlay_dataclass(t):
    return "overlay" in getattr(t, "__metadata__", ())

def is_custom_init_field(f : Field):
    return "custom_init" in getattr(f.type, "__metadata__", ())

def is_custom_init(f : Field):
    return "custom_init" in getattr(f, "__metadata__", ())


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
        try:
            return subclasses[cls_name]
        except KeyError:
            closest_matches = sorted(subclasses.keys(), key=lambda known_cls: Levenshtein.ratio(cls_name, known_cls), reverse=True)
            raise ConfigLineError(
                context,
                key,
                datacls,
                input_dict,
                f"Config yaml entry for section '{datacls.__name__}' invalid"
                f" name: '{cls_name}' could not be found as a subclass of {datacls.__name__}"
                f", did you mean one of {closest_matches[:3]}"
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

    if value is None:
        return []
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
                "Union types of multiple non-dataclasses are not allowed"
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


def parse_dict(context, key, _type, value):
    # If the type we've been given is just 'dict' with not further info, just return it
    if len(get_args(_type)) < 2:
        return {
            k:v for k, v in value.items()
            if k != LINE_KEY
        }

    key_type, value_type = get_args(_type)
    return {
        parse_field(context, "", key_type, k): parse_field(context, "", value_type, v)
        for k, v in value.items()
        if k != LINE_KEY
    }


def parse_field(context, key, _type, value):
    """Given a data class Field object or a bare type and a value object, try to parse it
    Strategy:
    1) if it's a dataclass use dataclass_from_dict
    2) if it's registered as a custom type, use that
    3) Otherwise, we hope it's something like like (str, int, Path...) where you
    can just call the type to construct the object from a string.

    """
    try:
        # print(f"parsing {key=}, {_type=}, {value=} {is_custom_init(_type)=}")
        if is_custom_init(_type):
            try:
                return _type.parse(value)
            except Exception as e:
                raise e from None

        if is_dataclass(_type):
            # possibly use a subclass instead of the base class
            datacls = determine_matching_dataclass(context, key, _type, value)
            return dataclass_from_dict(context, datacls, value)

        if is_union(_type):
            return parse_union(context, key, _type, value)

        if is_list(_type):
            return parse_list(context, key, _type, value)

        if is_literal(_type):
            return parse_literal(context, key, _type, value)

        if is_dict(_type):
            return parse_dict(context, key, _type, value)

        if isinstance(value, _type):
            return value
        
        if isinstance(value, dict):
            return _type(**{k : v for k,v in value.items() if k != "__line__"})
        
        return _type(value)


    except Exception as e:
        if isinstance(e, ConfigMatchError):
            raise e from None
        raise ConfigError(f"While parsing {key=}, {_type=}, {value=}") from e


@dataclass
class Context:
    filepath: Path | str
    subclasses: Subclasses = field(default_factory=Subclasses)
    overlay: bool = False # If we're in an overlay, ignore all missing keys and set values to None


def dataclass_from_dict(context, datacls, input_dict):
    "Given a JSON/YAML like dict of nested dicts/lists, parse it to datacls"
    # If at any point we encounter a type with an "overlay" annotation
    # Then from this point forward down the tre we will ignore missing keys
    if is_overlay_dataclass(datacls):
        context.overlay = True

    # Possibly match a subclass of datacls
    datacls = determine_matching_dataclass(context, None, datacls, input_dict)

    # Err if any keys are missing, there are extra keys etc
    check_matching(context, datacls, input_dict)
    
    kwargs = {
            field.name: parse_field(context, field.name, field.type, input_dict[field.name])
                        if field.name in input_dict else MISSING_OVERLAY # If the field is missing and it's an overlay, set it to None
            for field in fields(datacls)
            if field.name in input_dict or (context.overlay and field.init) # If it's not an overlay, skip this field
        }

    return datacls(**kwargs)


def parse_config_from_dict(datacls, input_dict, filepath: Path | str | None = None, overlay = False):
    context = Context(filepath or "???", overlay=overlay)
    return dataclass_from_dict(context, datacls, input_dict)

def parse_subclass_from_dict(cls, input_dict):
    context = Context("N/A")
    return dataclass_from_dict(context, cls, input_dict)


def merge_overlay(object, overlay):
    if not is_dataclass(object):
        return overlay if overlay is not MISSING_OVERLAY else object
    
    replacements = {
        field.name: merge_overlay(getattr(object, field.name), getattr(overlay, field.name, MISSING_OVERLAY))
        for field in fields(object) if field.init
    }

    return dataclasses.replace(object, **replacements)