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
from ..core.source import Source
from .acronet import AcronetSource, AddAcronetMetadata
from .meteotracker.source import MeteoTrackerSource
from .sensor_community import SensorCommunitySource
from .smart_citizen_kit import SmartCitizenKitSource
from .watch_directory import WatchDirectorySource
