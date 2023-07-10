# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

from obsproc.core.config_parser import parse_config
from pathlib import Path


def test_config_parser():
    for p in Path("../examples").glob("*.yaml"):
        print(p)
        parse_config(p)
