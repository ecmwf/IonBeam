# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

from pathlib import Path

from ionbeam.core.config_parser import parse_config


def test_config_parser():
    for p in Path("../examples").glob("*.yaml"):
        print(p)
        parse_config(p)

    yaml_file = (Path(__file__) / "../../../test_data/config/ionbeam").resolve()
    parse_config(yaml_file)

/Users/math/git/IonBeam-Deployment/docker/IonBeam/test/test_data/config/ionbeam