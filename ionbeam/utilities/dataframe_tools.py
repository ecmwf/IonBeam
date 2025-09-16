import pandas as pd

from ..core.constants import LatitudeColumn, LongitudeColumn, ObservationTimestampColumn
from ..models.models import DataIngestionMap


def coerce_types(df: pd.DataFrame, ingestion_map: DataIngestionMap, use_canonical_names = False):
    """
    Aligns the dataframes dtypes to the types specified by the DataIngestionMap
        - This help minimise the pain when loading pandas DFs from different sources which handle dtypes slightly differently (e.g, influxdb, parquet, etc..)
    """
    src_time = ObservationTimestampColumn if use_canonical_names or ingestion_map.datetime.from_col is None else ingestion_map.datetime.from_col
    src_lat = LatitudeColumn if use_canonical_names or ingestion_map.lat.from_col is None else ingestion_map.lat.from_col
    src_lon = LongitudeColumn if use_canonical_names or ingestion_map.lon.from_col is None else ingestion_map.lon.from_col
    
    df[src_time] = pd.to_datetime(df[src_time], utc=True, errors="coerce")
    df[src_lat] = pd.to_numeric(df[src_lat], errors="coerce").astype("float64")
    df[src_lon] = pd.to_numeric(df[src_lon], errors="coerce").astype("float64")

    all_vars = ingestion_map.canonical_variables + ingestion_map.metadata_variables
    keep_cols = [src_time, src_lat, src_lon]

    for var in all_vars:
        col = var.to_canonical_name() if use_canonical_names else var.column
        if col in df.columns:
            df[col] = df[col].astype(var.dtype, errors='raise')
            keep_cols.append(col)

    df = df[keep_cols]
    return df