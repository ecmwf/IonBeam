# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from datetime import timedelta

from ionbeam_client.models import (
    CanonicalVariable,
    DataIngestionMap,
    DatasetMetadata,
    IngestionMetadata,
    LatitudeAxis,
    LongitudeAxis,
    MetadataVariable,
    TimeAxis,
)

netatmo_metadata: IngestionMetadata = IngestionMetadata(
    dataset=DatasetMetadata(
        name="netatmo",
        aggregation_span=timedelta(hours=1),
        subject_to_change_window=timedelta(hours=0),
        description="IoT NetAtmo data collected from Met No",
        source_links=[],
        keywords=["netatmo", "iot", "data"],
    ),
    ingestion_map=DataIngestionMap(
        datetime=TimeAxis(),
        lat=LatitudeAxis(standard_name="latitude", cf_unit="degrees_north"),
        lon=LongitudeAxis(standard_name="longitude", cf_unit="degrees_east"),
        canonical_variables=[
            CanonicalVariable(
                column="air_temperature:2.0:point:PT0S",
                standard_name="air_temperature",
                cf_unit="degC",
                level=2.0,
                method="point",
                period="PT0S",
            ),
            CanonicalVariable(
                column="relative_humidity:2.0:point:PT0S",
                standard_name="relative_humidity",
                cf_unit="1",
                level=2.0,
                method="point",
                period="PT0S",
            ),
            CanonicalVariable(
                column="surface_air_pressure:2.0:point:PT0S",
                standard_name="surface_air_pressure",
                cf_unit="hPa",
                level=2.0,
                method="point",
                period="PT0S",
            ),
            CanonicalVariable(
                column="wind_from_direction:2.0:mean:PT5M",
                standard_name="wind_from_direction",
                cf_unit="degree",
                level=2.0,
                method="mean",
                period="PT5M",
            ),
            CanonicalVariable(
                column="wind_speed:2.0:mean:PT5M",
                standard_name="wind_speed",
                cf_unit="m s-1",
                level=2.0,
                method="mean",
                period="PT5M",
            ),
            CanonicalVariable(
                column="wind_speed_of_gust:2.0:mean:PT5M",
                standard_name="wind_speed_of_gust",
                cf_unit="m s-1",
                level=2.0,
                method="mean",
                period="PT5M",
            ),
            CanonicalVariable(
                column="precipitation_amount:2.0:sum:PT15H",
                standard_name="precipitation_amount",
                cf_unit="kg/m2",
                level=2.0,
                method="sum",
                period="PT15H",
            ),
        ],
        metadata_variables=[
            MetadataVariable(column="station_id"),
        ],
    ),
    version=1,
)
