import logging
import pathlib
from collections import defaultdict
from datetime import timedelta
from typing import Dict, List, Optional

import cf_units
import numpy as np
import pandas as pd
import pyodc as odc
from pydantic import BaseModel

from ionbeam.core.handler import BaseHandler

from ...models.models import CanonicalStandard, CanonicalVariable, DataSetAvailableEvent, DatasetMetadata

logger = logging.getLogger(__name__)

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
    108: 'Pa',
    107: 'Pa',
    39: 'K',
    40: 'K',
    58: '1',
    62: 'm',
    111: 'degree',
    112: 'm s-1',
    261: 'm s-1m',
    # 110: 'Pa',
}

class ODBProjectionService(BaseHandler[DataSetAvailableEvent, None]):
    def __init__(self, config: ODBProjectionServiceConfig):
        super().__init__("ODBProjectionService")
        self.config = config
        
        # Create lookup dict from ODB mapping config (keeps config style readable)
        self.variable_lookup: Dict[CanonicalStandard, List[int]] = defaultdict(list)
        for mapping in config.variable_map:
                for canonical in mapping.mapped_from:
                    self.variable_lookup[canonical].append(mapping.varno)

    def _map_canonical_df_to_odb(self, metadata: DatasetMetadata, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        odb_frames: List[pd.DataFrame] = []

        # Extract header values
        ts = pd.to_datetime(df['datetime'], utc=True)
        date_hdr = ts.dt.strftime("%Y%m%d").astype(int)
        time_hdr = ts.dt.strftime("%H%M%S").astype(int)

        # station id if present, otherwise fallback to ionbeam_{dataset} or something?
        fallback_statid = f'ionbeam-{metadata.name}-unknown-station'
        if 'station_id' in df.columns:
            statid_series = df['station_id'].fillna(fallback_statid)
        else:
            statid_series = pd.Series(fallback_statid, index=df.index)

        # Constants - are these specific to invidual readings? SYNOP vs TEMP etc..
        obstype = 1  # Mimic SYNOP format for now
        reportype = 16002  # Automatic Land SYNOP
        codetype = 11  # Manual Land SYNOP
        group_id = 17  # taken from ionbeam-legacy (links to Conventional / CONV)

        expver = "xxxx"
        class_ = 2 #pk1int "rd"  # taken from ionbeam-legacy - long name = "Research department"
        stream = 1247 #pk1int int format "lwda"  # taken from ionbeam-legacy
        type_desc = 264 #pk1int "ob" / grib "oai"

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
                but were acquired as analysis input for ERA-20C. For such data sets the ’analysis window’ is by conven-
                tion 6 hours long, indicated by 0, 6, 12, and 18, and containing data from 3 hours before (exclusive) and
                three hours after (inclusive) these time stamps. This also corresponds to the 6-hour assimilation windows
                as used in the 20CR reanalysis. The format of antime is HHMMSS, while it is HH, or HH:MM:SS for
                the mars key"
        
         - we'll default time to 0 to make fdb happy for now...
        
        
        """
        base["andate@desc"] = date_hdr # TODO if a dataset spans multiple days - fdb won't like it
        base["antime@desc"] = "0"
        base["reportype@hdr"] = reportype
        base["obstype@hdr"] = obstype
        base["codetype@hdr"] = codetype
        base["groupid@hdr"] = group_id
        base["statid@hdr"] = statid_series
        base["lat@hdr"] = pd.to_numeric(df["lat"], errors="coerce")
        base["lon@hdr"] = pd.to_numeric(df["lon"], errors="coerce")
        base["date@hdr"] = date_hdr
        base["time@hdr"] = time_hdr

        # Iterate per canonical data column
        ignore_cols = {'datetime', 'lat', 'lon', 'station_id'}
        for col in df.columns:
            if col in ignore_cols:
                continue

            # Identify canonical variable (skip non-canonical cols)
            try:
                col_var = CanonicalVariable.from_canonical_name(col)
            except ValueError: # TODO don't like exceptions for business logic - check col is valid shape instead
                # most likely a metadata col - we can safety ignore
                logger.warning('skipping %s - not a CanonicalVariable', col)
                continue

            varnos = self.variable_lookup.get(CanonicalStandard.from_canonical_name(col), [])

            for varno in varnos:
                varno_unit = varno_units.get(varno)
                if varno_unit is None:
                    logger.error("No map found for col %s", col)
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
                    logger.error("Unit conversion failed for column %s from %s to %s: %s", col, col_var.cf_unit, varno_unit, e)
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
        logger.info("Processing dataset available event %s into ODB", event)
        dataset_path = pathlib.Path(event.dataset_location)
        if dataset_path.exists():
            df = pd.read_parquet(dataset_path) # TODO - implement Parquet streaming

            odb_df = self._map_canonical_df_to_odb(event.metadata, df)
            if(odb_df is None):
                logger.error("Unable to create valid ODB")
                return
            output_filename = dataset_path.stem + ".odb"
            
            # Ensure output directory exists
            self.config.output_path.mkdir(parents=True, exist_ok=True)
            
            odb_path = str(self.config.output_path / output_filename)
            odb_df.to_parquet(odb_path.replace(".odb", ".parquet")) # easy debugging
            odc.encode_odb(odb_df, odb_path)
            logger.info("Written odb file to %s", odb_path)
        else:
            logger.error("Dataset file not found: %s", dataset_path)

if __name__ == "__main__":
    
    # Toy DataFrame with canonical column names
    temp_col = "air_temperature__degC__0.1__mean__PT10M"                       # maps to varno 39
    wind_col = "wind_speed__m s-1__10.0__mean__PT10M"                           # maps to varno 112
    mslp_col = "air_pressure_at_mean_sea_level__hPa__1.0__mean__PT1M"          # maps to varno 110

    df = pd.DataFrame({
        "datetime": pd.to_datetime(["2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z"], utc=True),
        "lat": [50.7, 51.7],
        "lon": [7.1, 7.2],
        "station_id": ["A", "B"],
        temp_col: [12.3, np.nan],     # degC -> K
        wind_col: [5.5, 3.2],         # m s-1 -> m s-1
        mslp_col: [1012.4, np.nan],   # hPa -> Pa
    })


    config = ODBProjectionServiceConfig(
        input_path=pathlib.Path("./raw-data"),
        output_path=pathlib.Path("./raw-data"),
        variable_map=[
            VarNoMapping(
                varno=39,
                mapped_from=[CanonicalStandard(
                    standard_name="air_temperature",
                    level=0.1,
                    method="mean",
                    period="PT10M"
                )]
            ),
            VarNoMapping(
                varno=112,
                mapped_from=[CanonicalStandard(
                    standard_name="wind_speed",
                    level=10.0,
                    method="mean",
                    period="PT10M",
                )]
            ),
            VarNoMapping(
                varno=110,
                mapped_from=[CanonicalStandard(
                    standard_name="air_pressure_at_mean_sea_level",
                    level=1.0,
                    method="mean",
                    period="PT1M"
                )]
            )
        ]
    )

    odb_service = ODBProjectionService(config)
    
    metadata = dataset=DatasetMetadata(
        name="test",
        description="",
        subject_to_change_window=timedelta(days=1),
        aggregation_span=timedelta(days=1),
        source_links=[],
        keywords=["meteotracker", "iot", "data"],
    )

    out = odb_service._map_canonical_df_to_odb(metadata, df)
    
    print(df.head())
    print(out.head())