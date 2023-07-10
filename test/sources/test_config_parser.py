from obsproc.core.config_parser import parse_config
from pathlib import Path


def test_config_parser():
    for p in Path("../examples").glob("*.yaml"):
        print(p)
        parse_config(p)
