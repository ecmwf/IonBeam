#!/usr/bin/env python3

import os

from obsproc.sources import load_source
from obsproc.sources.multi_file import MultiFileSource

# import pytest

examples_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "examples")

config = {
    "name": "multi-file",
    "basepath": examples_dir,
    "paths": [
        os.path.join(examples_dir, "test_data/file1"),
        os.path.join(examples_dir, "test_data/group"),
    ],
}


def test_load():
    s = load_source(**config)
    assert isinstance(s, MultiFileSource)

    ids = {d.id for d in s}
    assert ids == {"test_data/file1", "test_data/group/file2", "test_data/group/file3"}


def test_change_basedir():
    cfg = config.copy()
    del cfg["name"]
    cfg["basepath"] = os.path.join(cfg["basepath"], "test_data")
    s = MultiFileSource(**cfg)

    ids = {d.id for d in s}
    assert ids == {"file1", "group/file2", "group/file3"}
