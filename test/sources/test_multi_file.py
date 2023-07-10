#!/usr/bin/env python3

from pathlib import Path

from obsproc.sources import load_source
from obsproc.sources.multi_file import MultiFileSource

# import pytest

examples_dir = Path(__file__).parents[2] / "examples"

config = {"name": "multi-file", "basepath": examples_dir, "source": "test_datasource"}

test_paths = [
    ("test_data/file1", {"test_data/file1"}),  # a file
    (
        "test_data/group",
        {"test_data/group/file2", "test_data/group/file3"},
    ),  # a directory
    (
        "test_data/**/file*",
        {
            "test_data/file1",
            "test_data/group/file2",
            "test_data/group/file3",
            "test_data/group/nested_dir/file4",
        },
    ),  # a glob pattern
    (
        "test_data/*",
        {"test_data/file1"},
    ),  # a glob pattern that should not include test_data/group/
]


def test_load():
    for pattern, expectation in test_paths:
        s = load_source(**config, paths=[pattern])
        assert isinstance(s, MultiFileSource)
        paths = {str(d.metadata.filepath.relative_to(config["basepath"])) for d in s}
        assert paths == expectation


def test_multi_load():
    s = load_source(**config, paths=["test_data/file1", "test_data/group"])
    assert isinstance(s, MultiFileSource)
    paths = {str(d.metadata.filepath.relative_to(config["basepath"])) for d in s}
    assert paths == {
        "test_data/file1",
        "test_data/group/file2",
        "test_data/group/file3",
    }
