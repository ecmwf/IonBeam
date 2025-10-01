import pathlib
from collections import defaultdict
from datetime import timedelta
from typing import Dict, List, Optional

import cf_units
import pandas as pd
import pyodc as odc
from pydantic import BaseModel

from ionbeam.core.constants import LatitudeColumn, LongitudeColumn, ObservationTimestampColumn
from ionbeam.core.handler import BaseHandler
from ionbeam.models.models import CanonicalStandard, CanonicalVariable, DataSetAvailableEvent, DatasetMetadata
from ionbeam.observability.metrics import IonbeamMetricsProtocol


class VarNoMapping(BaseModel):
    varno: int
    mapped_from: List[CanonicalStandard]


class ODBProjectionServiceConfig(BaseModel):
    input_path: pathlib.Path
    output_path: pathlib.Path
    variable_map: List[VarNoMapping]


# Ionbeams CanonicalVariables are stored in their raw (cf_unit compliant) unit
# we can offload conversions to cf_units (https://cf-units.readthedocs.io/en/latest/unit.html) but we need to know what ODB expects
varno_units = {
    108: "Pa",
    107: "Pa",
    39: "K",
    40: "K",
    58: "1",
    62: "m",
    111: "degree",
    112: "m s-1",
    261: "m s-1",
    # 110: 'Pa',
}


class ODBProjectionService(BaseHandler[DataSetAvailableEvent, None]):
    def __init__(self, config: ODBProjectionServiceConfig, metrics: IonbeamMetricsProtocol):
        super().__init__("ODBProjectionService", metrics)
        self.config = config

        # Create lookup dict from ODB mapping config (keeps config style readable)
        self.variable_lookup: Dict[CanonicalStandard, List[int]] = defaultdict(list)
        for mapping in config.variable_map:
            for canonical in mapping.mapped_from:
                self.variable_lookup[canonical].append(mapping.varno)

    def _map_canonical_df_to_odb(self, metadata: DatasetMetadata, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        odb_frames: List[pd.DataFrame] = []

        # Extract header values
        ts = pd.to_datetime(df[ObservationTimestampColumn], utc=True)
        date_hdr = ts.dt.strftime("%Y%m%d").astype(int)
        time_hdr = ts.dt.strftime("%H%M%S").astype(int)

        # station id if present, otherwise fallback to ionbeam_{dataset} or something?
        fallback_statid = f"ionbeam-{metadata.name}-unknown-station"
        if "station_id" in df.columns:
            statid_series = df["station_id"].fillna(fallback_statid)
        else:
            statid_series = pd.Series(fallback_statid, index=df.index)

        # Constants - are these specific to invidual readings? SYNOP vs TEMP etc..
        obstype = 1  # Mimic SYNOP format for now
        reportype = 16002  # Automatic Land SYNOP
        codetype = 11  # Manual Land SYNOP
        group_id = 17  # taken from ionbeam-legacy (links to Conventional / CONV)

        expver = "xxxx"
        class_ = 2  # pk1int "rd"  # taken from ionbeam-legacy - long name = "Research department"
        stream = 1247  # pk1int int format "lwda"  # taken from ionbeam-legacy
        type_desc = 264  # pk1int "ob" / grib "oai"

        # Base frame with header/MARS qualifiers (aligned by index; per-row values kept)
        base = pd.DataFrame(index=df.index)
        base["expver@desc"] = expver
        base["class@desc"] = class_
        base["stream@desc"] = stream
        base["type@desc"] = type_desc
        base["creaby@desc"] = "Ionbeam"  # TODO - add version info once properly deployed
        """
        # TODO - confirm what andate + antime should actually be 
        #   - ionbeam-legacy used 'msg.data.time.dt' which is difficult to trace
        #   -  "These two keys specify an assimilation window, which groups together all observations that were con-
                sidered during such a window. The corresponding ODB columns (both INTEGER, see Table 3) are
                andate and antime rather than date and time, since, confusingly, these latter indicate the actual tim-
                ing of the observation. The ERA-20C reanalysis, (Poli et al., 2013), uses an assimilation window of 24
                hours. It is always labelled by antime=0 and contains data from 9 UTC (exclusive) the previous day
                until 9 UTC (inclusive). The ISPDv2.2 and ICOADSv2.5 are not the result of an in-house assimilation,
                but were acquired as analysis input for ERA-20C. For such data sets the 'analysis window' is by conven-
                tion 6 hours long, indicated by 0, 6, 12, and 18, and containing data from 3 hours before (exclusive) and
                three hours after (inclusive) these time stamps. This also corresponds to the 6-hour assimilation windows
                as used in the 20CR reanalysis. The format of antime is HHMMSS, while it is HH, or HH:MM:SS for
                the mars key"
        
         - we'll default time to 0 to make fdb happy for now...
        
        
        """
        base["andate@desc"] = date_hdr  # TODO if a dataset spans multiple days - fdb won't like it
        base["antime@desc"] = "0"
        base["reportype@hdr"] = reportype
        base["obstype@hdr"] = obstype
        base["codetype@hdr"] = codetype
        base["groupid@hdr"] = group_id
        base["statid@hdr"] = statid_series
        base["lat@hdr"] = pd.to_numeric(df[LatitudeColumn], errors="coerce")
        base["lon@hdr"] = pd.to_numeric(df[LongitudeColumn], errors="coerce")
        base["date@hdr"] = date_hdr
        base["time@hdr"] = time_hdr

        # Iterate per canonical data column
        ignore_cols = {ObservationTimestampColumn, LatitudeColumn, LongitudeColumn, "station_id"}
        for col in df.columns:
            if col in ignore_cols:
                continue

            # Identify canonical variable (skip non-canonical cols)
            try:
                col_var = CanonicalVariable.from_canonical_name(col)
            except ValueError:  # TODO don't like exceptions for business logic - check col is valid shape instead
                # most likely a metadata col - we can safety ignore
                self.logger.warning("Skipping non-canonical column", column=col)
                continue

            varnos = self.variable_lookup.get(CanonicalStandard.from_canonical_name(col), [])
            
            if not varnos:
                self.logger.warning("No varno found", col=col_var)

            for varno in varnos:
                varno_unit = varno_units.get(varno)
                if varno_unit is None:
                    self.logger.error("Missing varno unit mapping", column=col)
                    continue

                # Ensure numeric values; keep NaN where not parseable
                values = pd.to_numeric(df[col], errors="coerce")
                mask = values.notna()
                if not mask.any():
                    continue  # nothing to emit for this column

                # Vectorised unit conversion
                try:
                    from_cf_unit = cf_units.Unit(col_var.cf_unit)
                    to_cf_unit = cf_units.Unit(varno_unit)
                    converted = from_cf_unit.convert(values[mask].to_numpy(), to_cf_unit)  # returns numpy.ndarray
                except Exception as e:
                    self.logger.error("Unit conversion failed", column=col, from_unit=col_var.cf_unit, to_unit=varno_unit, error=str(e))
                    continue  # Skip this column but continue to process others

                # Build this column's ODB slice (keep per-row headers for the masked rows)
                part = base.loc[mask].copy()
                part["varno@body"] = varno
                # TODO - work out how dtype should be handled here - do we need to cast to float? ionbeam default is double unless overridden by ingestion-map
                part["obsvalue@body"] = pd.Series(converted, index=part.index).astype(float)
                odb_frames.append(part)

        if not odb_frames:
            return None

        return pd.concat(odb_frames, ignore_index=True)

    async def _handle(self, event: DataSetAvailableEvent):
        dataset_path = pathlib.Path(event.dataset_location)
        if dataset_path.exists():
            df = pd.read_parquet(dataset_path)  # TODO - implement Parquet streaming

            odb_df = self._map_canonical_df_to_odb(event.metadata, df)
            if odb_df is None:
                self.logger.error("Unable to create valid ODB dataframe")
                return
            output_filename = dataset_path.stem + ".odb"

            # Ensure output directory exists
            odb_path = self.config.output_path
            odb_path.mkdir(parents=True, exist_ok=True)

            odb_df.to_parquet(str(odb_path / output_filename).replace(".odb", ".parquet"))  # easy debugging
            odc.encode_odb(odb_df, str(odb_path / output_filename))
            self.logger.info("Wrote ODB file", path=str(odb_path / output_filename))
        else:
            self.logger.error("Dataset file not found", path=str(dataset_path))


if __name__ == "__main__":
    # --- Use the actual-shaped sample you posted ---
    # (null -> None so pandas will set NaN)
    records = [
        {
            "datetime": 1758981644000,
            "lat": 49.2282,
            "lon": 8.779,
            "air_temperature__degC__2.0__point__PT0S": 15.4,
            "relative_humidity__1__2.0__point__PT0S": 80,
            "surface_air_pressure__hPa__2.0__point__PT0S": 1022.2,
            "wind_from_direction__deg__2.0__mean__PT5M": None,
            "wind_speed__m s-1__2.0__mean__PT5M": None,
            "wind_speed_of_gust__m/s__2.0__mean__PT5M": None,
            "station_id": "0-276-0-aaaf04f138b684df",
        },
        {
            "datetime": 1758981645000,
            "lat": 55.8518,
            "lon": 9.8376,
            "air_temperature__degC__2.0__point__PT0S": 16.7,
            "relative_humidity__1__2.0__point__PT0S": 61,
            "surface_air_pressure__hPa__2.0__point__PT0S": 1027.3,
            "wind_from_direction__deg__2.0__mean__PT5M": None,
            "wind_speed__m s-1__2.0__mean__PT5M": None,
            "wind_speed_of_gust__m/s__2.0__mean__PT5M": None,
            "station_id": "0-208-0-379eff8fd1d11043",
        },
        {
            "datetime": 1758981646000,
            "lat": 45.5873,
            "lon": 5.3144,
            "air_temperature__degC__2.0__point__PT0S": None,
            "relative_humidity__1__2.0__point__PT0S": None,
            "surface_air_pressure__hPa__2.0__point__PT0S": 1045.4,
            "wind_from_direction__deg__2.0__mean__PT5M": None,
            "wind_speed__m s-1__2.0__mean__PT5M": None,
            "wind_speed_of_gust__m/s__2.0__mean__PT5M": None,
            "station_id": "0-250-0-f81983d88ec59159",
        },
        {
            "datetime": 1758981648000,
            "lat": 48.5217,
            "lon": 10.8081,
            "air_temperature__degC__2.0__point__PT0S": 14.3,
            "relative_humidity__1__2.0__point__PT0S": 97,
            "surface_air_pressure__hPa__2.0__point__PT0S": 1023.5,
            "wind_from_direction__deg__2.0__mean__PT5M": None,
            "wind_speed__m s-1__2.0__mean__PT5M": None,
            "wind_speed_of_gust__m/s__2.0__mean__PT5M": None,
            "station_id": "0-276-0-08b9d356367e05bb",
        },
        {
            "datetime": 1758981657000,
            "lat": 62.1723,
            "lon": 27.8604,
            "air_temperature__degC__2.0__point__PT0S": 10.3,
            "relative_humidity__1__2.0__point__PT0S": 90,
            "surface_air_pressure__hPa__2.0__point__PT0S": 1029.9,
            "wind_from_direction__deg__2.0__mean__PT5M": None,
            "wind_speed__m s-1__2.0__mean__PT5M": None,
            "wind_speed_of_gust__m/s__2.0__mean__PT5M": None,
            "station_id": "0-246-0-22c57d7b6703bf7f",
        },
    ]

    # This is the actual-shaped raw DataFrame as in your feed
    raw = pd.DataFrame.from_records(records)

    # Canonical column names used by the mapper (keeping your constants)
    # NOTE: We convert epoch-ms "datetime" -> pandas UTC datetime in ObservationTimestampColumn,
    # and rename lat/lon into your canonical latitude/longitude columns.
    df = raw.rename(columns={"lat": LatitudeColumn, "lon": LongitudeColumn})

    print(df.columns)

    # Build a mapping tolerant to both surface and MSLP pressure names.
    config = ODBProjectionServiceConfig(
        input_path=pathlib.Path("./data-raw"),
        output_path=pathlib.Path("./data-raw"),
        variable_map=[
            VarNoMapping(
                varno=108,
                mapped_from=[
                    CanonicalStandard(
                        standard_name="air_pressure_at_mean_sea_level",
                        level=2.0,
                        method="mean",
                        period="PT1H",
                    )
                ],
            ),
            VarNoMapping(
                varno=107,
                mapped_from=[
                    CanonicalStandard(
                        standard_name="surface_air_pressure",
                        level=2.0,
                        method="point",
                        period="PT0S",
                    )
                ],
            ),
            VarNoMapping(
                varno=39,
                mapped_from=[
                    CanonicalStandard(
                        standard_name="air_temperature",
                        level=2.0,
                        method="point",
                        period="PT0S",
                    )
                ],
            ),
            VarNoMapping(
                varno=40,
                mapped_from=[
                    CanonicalStandard(
                        standard_name="dew_point_temperature",
                        level=2.0,
                        method="point",
                        period="PT0S",
                    )
                ],
            ),
            VarNoMapping(
                varno=58,
                mapped_from=[
                    CanonicalStandard(
                        standard_name="relative_humidity",
                        level=2.0,
                        method="point",
                        period="PT0S",
                    )
                ],
            ),
            VarNoMapping(
                varno=62,
                mapped_from=[
                    CanonicalStandard(
                        standard_name="visibility_in_air",
                        level=2.0,
                        method="point",
                        period="PT0S",
                    )
                ],
            ),
            VarNoMapping(
                varno=111,
                mapped_from=[
                    CanonicalStandard(
                        standard_name="wind_from_direction",
                        level=10.0,
                        method="mean",
                        period="PT1H",
                    )
                ],
            ),
            VarNoMapping(
                varno=112,
                mapped_from=[
                    CanonicalStandard(
                        standard_name="wind_speed",
                        level=10.0,
                        method="mean",
                        period="PT1H",
                    )
                ],
            ),
            VarNoMapping(
                varno=261,
                mapped_from=[
                    CanonicalStandard(
                        standard_name="wind_speed_of_gust",
                        level=10.0,
                        method="mean",
                        period="PT1H",
                    )
                ],
            ),
        ],
    )

    from ionbeam.observability.metrics import IonbeamMetrics
    metrics = IonbeamMetrics()
    odb_service = ODBProjectionService(config, metrics)

    metadata = DatasetMetadata(
        name="test",
        description="",
        subject_to_change_window=timedelta(days=1),
        aggregation_span=timedelta(days=1),
        source_links=[],
        keywords=["meteotracker", "iot", "data"],
    )

    out = odb_service._map_canonical_df_to_odb(metadata, df)

    print(df.head())
    print(out)
