# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

from ..core.bases import TabularMessage, Processor, Encoder
from ..core.plugins import find_plugin
from typing import Iterable
import os
import dataclasses


def load_encoder(name: str, **kwargs) -> Encoder:
    klass = find_plugin(os.path.dirname(__file__), __name__, "encoder", name)
    return klass(**kwargs)
