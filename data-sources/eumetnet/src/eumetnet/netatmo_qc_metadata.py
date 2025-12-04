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

netatmo_qc_metadata: IngestionMetadata = IngestionMetadata(
    dataset=DatasetMetadata(
        name="netatmo_qc",
        aggregation_span=timedelta(hours=1),
        subject_to_change_window=timedelta(hours=0),
        description="Quality-controlled NetAtmo data from FMI",
        source_links=[],
        keywords=["netatmo", "iot", "qc", "fmi"],
    ),
    ingestion_map=DataIngestionMap(
        datetime=TimeAxis(),
        lat=LatitudeAxis(standard_name="latitude", cf_unit="degrees_north"),
        lon=LongitudeAxis(standard_name="longitude", cf_unit="degrees_east"),
        canonical_variables=[
            CanonicalVariable(
                column="air_temperature:2.0:point:PT10M",
                standard_name="air_temperature",
                cf_unit="degC",
                level=2.0,
                method="point",
                period="PT10M",
            ),
            CanonicalVariable(
                column="surface_air_pressure:2.0:point:PT10M",
                standard_name="surface_air_pressure",
                cf_unit="hPa",
                level=2.0,
                method="point",
                period="PT10M",
            ),
            CanonicalVariable(
                column="precipitation_amount:2.0:point:PT10M",
                standard_name="precipitation_amount",
                cf_unit="kg m-2",
                level=2.0,
                method="point",
                period="PT10M",
            ),
        ],
        metadata_variables=[
            MetadataVariable(
                column="air_temperature:2.0:point:PT10M_qc", dtype="int64"
            ),
            MetadataVariable(
                column="surface_air_pressure:2.0:point:PT10M_qc", dtype="int64"
            ),
            MetadataVariable(
                column="precipitation_amount:2.0:point:PT10M_qc", dtype="int64"
            ),
            MetadataVariable(column="station_id"),
        ],
    ),
    version=1,
)
