import pathlib
import re
from collections import defaultdict
from typing import AsyncIterator, Dict, List, Optional

import cf_units
import pandas as pd
import pyarrow as pa
import pyodc as odc
import structlog
from pydantic import BaseModel

from ionbeam_client.constants import (
    LatitudeColumn,
    LongitudeColumn,
    ObservationTimestampColumn,
)
from ionbeam_client.models import (
    CanonicalStandard,
    CanonicalVariable,
    DataSetAvailableEvent,
)


logger = structlog.get_logger(__name__)


class VarNoMapping(BaseModel):
    """Mapping of ODB varno to canonical variables."""

    varno: int
    mapped_from: List[CanonicalStandard]


class ODBExporterConfig(BaseModel):
    """Configuration for ODB exporter."""

    output_path: pathlib.Path
    variable_map: List[VarNoMapping]


# ODB expected units for each varno
# See: https://confluence.ecmwf.int/display/UDOC/ODB+variable+numbers
VARNO_UNITS = {
    108: "Pa",  # MSLP
    107: "Pa",  # Surface pressure
    39: "K",  # Air temperature
    40: "K",  # Dew point temperature
    58: "1",  # Relative humidity
    62: "m",  # Visibility
    111: "degree",  # Wind direction
    112: "m s-1",  # Wind speed
    261: "m s-1",  # Wind gust
}


class ODBExporter:
    """ODB format exporter for ECMWF."""

    def __init__(self, config: ODBExporterConfig):
        """Initialize ODB exporter.

        Args:
            config: Exporter configuration
        """
        self.config = config
        self.logger = logger.bind(exporter="odb")

        # Create lookup dict from ODB mapping config
        self.variable_lookup: Dict[CanonicalStandard, List[int]] = defaultdict(list)
        for mapping in config.variable_map:
            for canonical in mapping.mapped_from:
                self.variable_lookup[canonical].append(mapping.varno)

    def _map_canonical_batch_to_odb(
        self, dataset_name: str, batch: pa.RecordBatch
    ) -> Optional[pa.Table]:
        df = batch.to_pandas(
            types_mapper={pa.string(): pd.StringDtype(storage="python")}.get
        )

        odb_frames: List[pd.DataFrame] = []

        ts = pd.to_datetime(df[ObservationTimestampColumn], utc=True)
        date_hdr = ts.dt.strftime("%Y%m%d").astype(int)
        time_hdr = ts.dt.strftime("%H%M%S").astype(int)

        fallback_statid = f"ionbeam-{dataset_name}-unknown-station"
        if "station_id" in df.columns:
            statid_series = df["station_id"].fillna(fallback_statid)
        else:
            statid_series = pd.Series(fallback_statid, index=df.index)

        # ODB constants
        obstype = 1  # SYNOP format
        reportype = 16002  # Automatic Land SYNOP
        codetype = 11  # Manual Land SYNOP
        group_id = 17  # Conventional data

        # Base frame with header/MARS qualifiers
        base = pd.DataFrame(index=df.index)
        base["expver@desc"] = "xxxx"
        base["class@desc"] = 2  # Research department
        base["stream@desc"] = 1247
        base["type@desc"] = 264
        base["creaby@desc"] = "Ionbeam"
        base["andate@desc"] = date_hdr
        base["antime@desc"] = 0
        base["reportype@hdr"] = reportype
        base["obstype@hdr"] = obstype
        base["codetype@hdr"] = codetype
        base["groupid@hdr"] = group_id
        base["wigosid@hdr"] = statid_series
        base["lat@hdr"] = pd.to_numeric(df[LatitudeColumn], errors="coerce")
        base["lon@hdr"] = pd.to_numeric(df[LongitudeColumn], errors="coerce")
        base["date@hdr"] = date_hdr
        base["time@hdr"] = time_hdr

        ignore_cols = {
            ObservationTimestampColumn,
            LatitudeColumn,
            LongitudeColumn,
            "station_id",
        }

        for col in df.columns:
            if col in ignore_cols:
                continue

            # Skip QC metadata columns - they will be processed alongside their data columns
            if col.endswith("_qc"):
                continue

            try:
                col_var = CanonicalVariable.from_canonical_name(col)
            except ValueError:
                self.logger.debug("Skipping non-canonical column", column=col)
                continue

            varnos = self.variable_lookup.get(
                CanonicalStandard.from_canonical_name(col), []
            )

            if not varnos:
                self.logger.debug("No varno mapping found", column=col)
                continue

            for varno in varnos:
                varno_unit = VARNO_UNITS.get(varno)
                if varno_unit is None:
                    self.logger.error(
                        "Missing varno unit mapping", column=col, varno=varno
                    )
                    continue

                values = pd.to_numeric(df[col], errors="coerce")
                mask = values.notna()
                if not mask.any():
                    continue

                try:
                    from_cf_unit = cf_units.Unit(col_var.cf_unit)
                    to_cf_unit = cf_units.Unit(varno_unit)
                    converted = from_cf_unit.convert(
                        values[mask].to_numpy(), to_cf_unit
                    )
                except Exception as e:
                    self.logger.error(
                        "Unit conversion failed",
                        column=col,
                        from_unit=col_var.cf_unit,
                        to_unit=varno_unit,
                        error=str(e),
                    )
                    continue

                part = base.loc[mask].copy()
                part["varno@body"] = varno
                part["obsvalue@body"] = pd.Series(converted, index=part.index).astype(
                    float
                )

                col_standard = CanonicalStandard.from_canonical_name(col)
                qc_col = next(
                    (
                        c
                        for c in df.columns
                        if re.match(
                            rf"^{re.escape(col_standard.standard_name)}.*_qc$", c
                        )
                    ),
                    None,
                )
                if qc_col:
                    self.logger.debug("Found QC column", column=col, qc_column=qc_col)
                    qc_values = pd.to_numeric(df[qc_col], errors="coerce")
                    part["quality@body"] = qc_values.loc[mask].fillna(0).astype(int)
                else:
                    part["quality@body"] = 0

                odb_frames.append(part)

        if not odb_frames:
            return None

        combined_df = pd.concat(odb_frames, ignore_index=True)
        return pa.Table.from_pandas(combined_df)

    async def export_handler(
        self, event: DataSetAvailableEvent, batch_stream: AsyncIterator[pa.RecordBatch]
    ) -> None:
        self.logger.info(
            "Starting ODB export",
            dataset=event.metadata.name,
            event_id=str(event.id),
            start=event.start_time.isoformat(),
            end=event.end_time.isoformat(),
        )

        odb_tables: List[pa.Table] = []

        async for batch in batch_stream:
            if batch.num_rows == 0:
                continue

            odb_table = self._map_canonical_batch_to_odb(event.metadata.name, batch)
            if odb_table is not None:
                odb_tables.append(odb_table)

        if not odb_tables:
            self.logger.warning(
                "No ODB data generated",
                dataset=event.metadata.name,
                event_id=str(event.id),
            )
            return

        combined_table = pa.concat_tables(odb_tables)
        odb_df = combined_table.to_pandas()

        start_str = event.start_time.strftime("%Y%m%d_%H%M")
        end_str = event.end_time.strftime("%Y%m%d_%H%M")
        output_filename = f"{event.metadata.name}_{start_str}_{end_str}.odb"

        odb_path = self.config.output_path
        odb_path.mkdir(parents=True, exist_ok=True)

        output_file = odb_path / output_filename
        odc.encode_odb(odb_df, str(output_file))
        odb_df.to_parquet(str(output_file).replace(".odb", ".parquet"))

        self.logger.info(
            "ODB export completed",
            dataset=event.metadata.name,
            output=str(output_file),
            rows=len(odb_df),
        )
