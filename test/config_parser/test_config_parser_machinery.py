import pytest
from ionbeam.core.config_parser_machinery import parse_config_from_dict, Subclasses

from dataclasses import dataclass
from pathlib import Path
from typing import List


def test_subclass_finder():
    class Animal:
        pass

    class Dog(Animal):
        pass

    class Cat(Animal):
        pass

    subclasses = Subclasses()
    assert set(subclasses.get(Animal)) == {"Animal", "Dog", "Cat"}


from ionbeam.core.bases import Source, Parser, Aggregator, Encoder


@pytest.mark.parametrize("cls", [Source, Parser, Aggregator, Encoder])
def test_subclass_finder_imported(cls):
    subclasses = Subclasses()
    # x > y where x and y are sets means x is a subset of y but y is bigger.
    assert set(subclasses.get(cls)) > {cls.__name__}


def test_basic_stuff():
    @dataclass
    class NestedDataclass:
        string: str

    @dataclass
    class Test:
        string: str
        path: Path
        nested_dataclass: NestedDataclass
        int_list: List[int]

    test = dict(
        string="string",
        path="/etc/config",
        nested_dataclass=dict(string="string1"),
        int_list=[1, 2, 3],
    )

    expectation = Test(
        string="string",
        path=Path("/etc/config"),
        nested_dataclass=NestedDataclass("string1"),
        int_list=[1, 2, 3],
    )

    d = parse_config_from_dict(Test, test)

    assert d == expectation


def test_unions():
    @dataclass
    class Time:
        seconds: float

    @dataclass
    class Space:
        meters: float

    @dataclass
    class Test:
        simple_union: Time | Space
        optional_union: Time | Space | None = None
        not_present_union: Time | Space | None = None
        disallowed_union: int | str | None = None

    test = dict(
        simple_union={"class": "Time", "seconds": 11},
        optional_union={"class": "Space", "meters": 500},
        # disallowed_union=1,
    )

    expectation = Test(
        simple_union=Time(11),
        optional_union=Space(500),
    )

    d = parse_config_from_dict(Test, test)

    assert d == expectation
