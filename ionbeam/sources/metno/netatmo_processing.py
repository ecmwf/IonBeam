import logging
from typing import AsyncIterator, List, Optional

import pandas as pd

_logger = logging.getLogger(__name__)


def process_netatmo_geojson_messages_to_df(buffer: List[dict], logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Transform a list of Netatmo-like GeoJSON Feature dicts into a wide DataFrame:
    - Deduplicates by (station_id, datetime, lat, lon, parameter) keeping latest by pubtime, then by arrival order.
    - Pivots parameters to columns.
    """
    log = logger or _logger
    log.info("prcessing %s messages...", len(buffer))
    rows = []
    for idx, d in enumerate(buffer):
        if not isinstance(d, dict):
            log.warning("chunk[%d]: not a mapping; skipping", idx)
            continue

        props = d.get("properties")
        if not isinstance(props, dict):
            log.warning("chunk[%d]: missing 'properties'; skipping", idx)
            continue

        content = props.get("content")
        if not isinstance(content, dict):
            log.warning("chunk[%d]: missing 'properties.content'; skipping", idx)
            continue

        geom = d.get("geometry")
        if not isinstance(geom, dict):
            log.warning("chunk[%d]: missing 'geometry'; skipping", idx)
            continue

        station_id = props.get("platform")
        if not station_id:
            log.warning("chunk[%d]: missing 'properties.platform'; skipping", idx)
            continue

        obs_time = pd.to_datetime(props.get("datetime"), utc=True, errors="coerce")
        if pd.isna(obs_time):
            log.warning("chunk[%d]: invalid 'properties.datetime'; skipping", idx)
            continue

        pub_time = pd.to_datetime(props.get("pubtime"), utc=True, errors="coerce")

        standard_name = content.get("standard_name")
        if not standard_name:
            log.warning("chunk[%d]: missing 'content.standard_name'; skipping", idx)
            continue

        # Build parameter from available parts (no extra sanitization)
        level = props.get("level")
        method = props.get("function")
        period = props.get("period")
        parts = [standard_name, level, method, period]
        param = ":".join([str(p) for p in parts if p not in (None, "")])

        coords = geom.get("coordinates")
        if not isinstance(coords, dict):
            log.warning("chunk[%d]: 'geometry.coordinates' must be a mapping with lat/lon; skipping", idx)
            continue

        try:
            lat = float(coords.get("lat"))
            lon = float(coords.get("lon"))
        except (TypeError, ValueError):
            log.warning("chunk[%d]: invalid lat/lon; skipping", idx)
            continue

        value = pd.to_numeric(content.get("value"), errors="coerce")
        # Keep NaN values; they will show up as NaN in the pivot.

        rows.append(
            {
                "_row_idx": idx,  # arrival order within this chunk (fallback tie-break)
                "station_id": station_id,
                "datetime": obs_time,
                "lat": lat,
                "lon": lon,
                "pubtime": pub_time,  # may be NaT
                "parameter": param,
                "value": value,
            }
        )

    if not rows:
        return pd.DataFrame(columns=["station_id", "datetime", "lat", "lon"])

    df = pd.DataFrame(rows)
    key = ["station_id", "datetime", "lat", "lon", "parameter"]

    # Keep the latest by pubtime; if equal/missing pubtime, prefer latest arrival.
    # We sort so the desired record is LAST, then drop_duplicates(keep="last").
    df["has_pubtime"] = df["pubtime"].notna()
    df = df.sort_values(
        by=["has_pubtime", "pubtime", "_row_idx"],
        ascending=[True, True, True],  # NaT/False first, earlier pubtime first, earlier arrival first
        kind="stable",
    )
    dedup = df.drop_duplicates(subset=key, keep="last").drop(columns=["has_pubtime", "_row_idx"])

    # Pivot to wide
    wide = dedup.pivot(index=["station_id", "datetime", "lat", "lon"], columns="parameter", values="value").reset_index()
    wide.columns.name = None
    wide = wide.sort_values(["station_id", "datetime"]).reset_index(drop=True)

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
    async for msg in message_stream:
        buffer.append(msg)
        if len(buffer) >= batch_size:
            df = process_netatmo_geojson_messages_to_df(buffer, log)
            if df is not None and not df.empty:
                yield df
            buffer.clear()
    if buffer:
        df = process_netatmo_geojson_messages_to_df(buffer, log)
        if df is not None and not df.empty:
            yield df


