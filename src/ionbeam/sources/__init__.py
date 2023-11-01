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
from ..core.plugins import find_plugin
from ..core.bases import Source

from .meteotracker.online_source import MeteoTrackerSource
from .meteotracker.offline_source import MeteoTrackerOfflineSource
from .cima.source import CIMASource
from .multi_file import MultiFileSource
from .watch_directory import WatchDirectorySource

__all__ = [
    "MeteoTrackerSource",
    "MeteoTrackerOfflineSource",
    "CIMASource",
    "MultiFileSource",
    "WatchDirectorySource",
]


def load_source(name: str, **kwargs) -> Source:
    klass = find_plugin(Path(__file__).parent, __name__, "source", name)
    return klass(**kwargs)
