import logging
import time
from typing import AsyncIterator, List, Optional

import pandas as pd

_logger = logging.getLogger(__name__)


def process_netatmo_geojson_messages_to_df(buffer: List[dict], logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Transform a list of Netatmo-like GeoJSON Feature dicts into a wide DataFrame:
    - Deduplicate by (station_id, datetime, lat, lon, parameter) keeping latest by pubtime, then by arrival order.
    - Pivot parameters to columns.
    """
    log = logger or _logger
    t0 = time.perf_counter()

    records: List[dict] = []
    invalid_count = 0

    for idx, d in enumerate(buffer):
        try:
            if not isinstance(d, dict):
                raise ValueError("not mapping")

            props = d.get("properties") or {}
            if not isinstance(props, dict):
                raise ValueError("no properties")

            content = props.get("content") or {}
            if not isinstance(content, dict):
                raise ValueError("no content")

            geom = d.get("geometry") or {}
            if not isinstance(geom, dict):
                raise ValueError("no geometry")

            coords = geom.get("coordinates") or {}
            if not isinstance(coords, dict):
                raise ValueError("no coordinates")

            station_id = props.get("platform")
            dt_raw = props.get("datetime")
            standard_name = content.get("standard_name")

            if not station_id or dt_raw is None or not standard_name:
                raise ValueError("missing essentials")

            lat_raw = coords.get("lat")
            lon_raw = coords.get("lon")

            # Build parameter from available parts
            parts = [standard_name, props.get("level"), props.get("function"), props.get("period")]
            parameter = ":".join(str(p) for p in parts if p not in (None, ""))

            records.append(
                {
                    "_row_idx": idx,  # arrival order within this chunk (fallback tie-break)
                    "station_id": station_id,
                    "datetime": dt_raw,
                    "lat": lat_raw,
                    "lon": lon_raw,
                    "pubtime": props.get("pubtime"),
                    "parameter": parameter,
                    "value": content.get("value"),
                }
            )
        except Exception:
            invalid_count += 1
            continue

    if not records:
        return pd.DataFrame(columns=["station_id", "datetime", "lat", "lon"])

    # Create DataFrame and coerce types
    df = pd.DataFrame.from_records(records)
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["pubtime"] = pd.to_datetime(df["pubtime"], utc=True, errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Drop rows with invalid essential fields. Keep NaN values in 'value' like before.
    before = len(df)
    df = df.dropna(subset=["station_id", "datetime", "lat", "lon"])
    dropped = before - len(df)
    if dropped or invalid_count:
        log.info("Parsed %d records; dropped %d after coercion; invalid during parse=%d", before, dropped, invalid_count)

    # Reduce memory for string columns
    df["station_id"] = df["station_id"].astype("category")
    df["parameter"] = df["parameter"].astype("category")

    key = ["station_id", "datetime", "lat", "lon", "parameter"]

    # Log conflicts
    conflicts_count = 0
    try:
        val_nunique = df.groupby(key, sort=False, observed=True)["value"].nunique(dropna=True)
        conflicts_count = int((val_nunique > 1).sum())
        if conflicts_count:
            log.warning(
                "Detected %d conflicting observations for identical keys; keeping latest by pubtime/arrival",
                conflicts_count,
            )
    except Exception as e:
        log.debug("Conflict analysis failed: %s", e)

    # Keep the latest by pubtime; if equal/missing pubtime, prefer latest arrival.
    # We sort so the desired record is LAST, then drop_duplicates(keep='last').
    df["has_pubtime"] = df["pubtime"].notna()
    df = df.sort_values(
        by=["has_pubtime", "pubtime", "_row_idx"],
        ascending=[True, True, True],
        kind="stable",
    )
    dedup = df.drop_duplicates(subset=key, keep="last").drop(columns=["has_pubtime", "_row_idx"])

    # Pivot to wide
    wide = (
        dedup.pivot(index=["station_id", "datetime", "lat", "lon"], columns="parameter", values="value")
        .reset_index()
        .sort_values(["station_id", "datetime"])
        .reset_index(drop=True)
    )
    wide.columns.name = None

    elapsed = time.perf_counter() - t0
    log.info(
        "netatmo batch processed: parsed=%d dropped=%d conflicts=%d out_rows=%d columns=%d elapsed=%.2fs",
        before,
        dropped,
        conflicts_count,
        len(wide),
        wide.shape[1],
        elapsed,
    )
    return wide


async def netatmo_dataframe_stream(
    message_stream: AsyncIterator[dict],
    batch_size: int = 50000,
    logger: Optional[logging.Logger] = None,
) -> AsyncIterator[pd.DataFrame]:
    """
    Consume an async stream of GeoJSON messages and yield pivoted DataFrames in batches.
    Deduplication is performed within each batch.
    """
    log = logger or _logger
    buffer: List[dict] = []
    total_msgs = 0
    total_batches = 0
    stream_start = time.perf_counter()
    async for msg in message_stream:
        buffer.append(msg)
        if len(buffer) >= batch_size:
            df = process_netatmo_geojson_messages_to_df(buffer, log)
            total_batches += 1
            total_msgs += len(buffer)
            rows = 0 if df is None else len(df)
            log.debug("netatmo_dataframe_stream batch %d: input_msgs=%d output_rows=%d", total_batches, len(buffer), rows)
            if df is not None and not df.empty:
                yield df
            buffer.clear()
    if buffer:
        df = process_netatmo_geojson_messages_to_df(buffer, log)
        total_batches += 1
        total_msgs += len(buffer)
        rows = 0 if df is None else len(df)
        log.debug("netatmo_dataframe_stream batch %d: input_msgs=%d output_rows=%d", total_batches, len(buffer), rows)
        if df is not None and not df.empty:
            yield df
    stream_elapsed = time.perf_counter() - stream_start
    log.info("netatmo_dataframe_stream complete: batches=%d total_input_msgs=%d elapsed=%.2fs", total_batches, total_msgs, stream_elapsed)


