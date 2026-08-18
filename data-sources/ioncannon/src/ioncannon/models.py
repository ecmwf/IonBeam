# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

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
