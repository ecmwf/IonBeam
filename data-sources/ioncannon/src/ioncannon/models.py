# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from pydantic import BaseModel


class IonCannonConfig(BaseModel):
    # Core parameters
    num_stations: int = 1000
    measurement_frequency_minutes: int = 5

    # Geographic bounds for station placement (Central Europe)
    min_lat: float = 47.0
    max_lat: float = 55.0
    min_lon: float = 5.0
    max_lon: float = 15.0

    # Cardinality control for metadata variables
    metadata_cardinality: int = 10
