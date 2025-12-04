import asyncio
import logging
import time
from collections.abc import AsyncIterable, Iterable
from typing import AsyncIterator, List, Optional

import pandas as pd
import pyarrow as pa
import structlog

from ionbeam_client.arrow_tools import dataframes_to_record_batches

_logger = structlog.get_logger(__name__)


async def _iter_messages(
    message_stream: Iterable[dict] | AsyncIterable[dict],
) -> AsyncIterator[dict]:
    if isinstance(message_stream, AsyncIterable):
        async for msg in message_stream:
            yield msg
    else:
        for msg in message_stream:
            yield msg
            await asyncio.sleep(0)


def process_netatmo_geojson_messages_to_df(
    messages: List[dict], logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Transform a list of Netatmo-like GeoJSON Feature dicts into a wide DataFrame:
    - Deduplicate by (station_id, datetime, lat, lon, parameter) keeping latest by pubtime, then by arrival order
    """
    log = logger or _logger
    t0 = time.perf_counter()

    records: List[dict] = []
    invalid_count = 0

    for idx, d in enumerate(messages):
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
            parts = [
                standard_name,
                props.get("level"),
                props.get("function"),
                props.get("period"),
            ]
            parameter = ":".join(str(p) for p in parts if p not in (None, ""))

            # Extract quality code
            quality_code = props.get("quality_code")

            records.append(
                {
                    "_row_idx": idx,  # arrival order within this chunk (fallback tie-break on collisions)
                    "station_id": station_id,
                    "datetime": dt_raw,
                    "lat": lat_raw,
                    "lon": lon_raw,
                    "pubtime": props.get("pubtime"),
                    "parameter": parameter,
                    "value": content.get("value"),
                    "qc_code": quality_code,
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
    # Keep qc_code as-is (can be None/NaN or integer)
    if "qc_code" in df.columns:
        df["qc_code"] = pd.to_numeric(df["qc_code"], errors="coerce")

    # Drop rows with invalid essential fields. Keep NaN values in 'value' and 'qc_code'.
    before = len(df)
    df = df.dropna(subset=["station_id", "datetime", "lat", "lon"])
    dropped = before - len(df)
    if dropped or invalid_count:
        log.info(
            "Parsed %d records; dropped %d after coercion; invalid during parse=%d",
            before,
            dropped,
            invalid_count,
        )

    # Reduce memory for string columns
    df["station_id"] = df["station_id"].astype("category")
    df["parameter"] = df["parameter"].astype("category")

    key = ["station_id", "datetime", "lat", "lon", "parameter"]

    # Log conflicts
    conflicts_count = 0
    try:
        val_nunique = df.groupby(key, sort=False, observed=True)["value"].nunique(
            dropna=True
        )
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
    dedup = df.drop_duplicates(subset=key, keep="last").drop(
        columns=["has_pubtime", "_row_idx"]
    )

    # Pivot values to wide format
    wide_values = dedup.pivot(
        index=["station_id", "datetime", "lat", "lon"],
        columns="parameter",
        values="value",
    )

    # Only process QC codes if the column exists in the deduped data
    if "qc_code" in dedup.columns:
        # Pivot QC codes to wide format
        wide_qc = dedup.pivot(
            index=["station_id", "datetime", "lat", "lon"],
            columns="parameter",
            values="qc_code",
        )

        # Rename QC columns to add _qc suffix
        wide_qc.columns = [f"{col}_qc" for col in wide_qc.columns]

        # Combine value and QC columns
        wide = pd.concat([wide_values, wide_qc], axis=1).reset_index()

        # Convert QC columns to int64, filling NaN with 0
        qc_cols = [col for col in wide.columns if col.endswith("_qc")]
        for col in qc_cols:
            wide[col] = wide[col].fillna(0).astype("int64")
    else:
        wide = wide_values.reset_index()
    wide = wide.sort_values(["station_id", "datetime"]).reset_index(drop=True)
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
    message_stream: Iterable[dict] | AsyncIterable[dict],
    batch_size: int = 50000,
    logger: Optional[logging.Logger] = None,
) -> AsyncIterator[pd.DataFrame]:
    """
    Consume a stream of GeoJSON messages (iterable or async iterable) and yield pivoted DataFrames in batches.
    Deduplication is performed within each batch.
    """
    log = logger or _logger
    buffer: List[dict] = []
    total_msgs = 0
    total_batches = 0
    stream_start = time.perf_counter()
    async for msg in _iter_messages(message_stream):
        buffer.append(msg)
        if len(buffer) >= batch_size:
            df = process_netatmo_geojson_messages_to_df(buffer, log)
            total_batches += 1
            total_msgs += len(buffer)
            rows = 0 if df is None else len(df)
            log.debug(
                "netatmo_dataframe_stream batch %d: input_msgs=%d output_rows=%d",
                total_batches,
                len(buffer),
                rows,
            )
            if df is not None and not df.empty:
                yield df
            buffer.clear()
    if buffer:
        df = process_netatmo_geojson_messages_to_df(buffer, log)
        total_batches += 1
        total_msgs += len(buffer)
        rows = 0 if df is None else len(df)
        log.debug(
            "netatmo_dataframe_stream batch %d: input_msgs=%d output_rows=%d",
            total_batches,
            len(buffer),
            rows,
        )
        if df is not None and not df.empty:
            yield df
    stream_elapsed = time.perf_counter() - stream_start
    log.info(
        "Netatmo dataframe stream complete",
        batches=total_batches,
        total_input_msgs=total_msgs,
        elapsed=stream_elapsed,
    )


async def netatmo_record_batch_stream(
    message_stream: Iterable[dict] | AsyncIterable[dict],
    *,
    batch_size: int = 50000,
    logger: Optional[logging.Logger] = None,
    schema: pa.Schema | None = None,
    preserve_index: bool = False,
) -> AsyncIterator[pa.RecordBatch]:
    """
    Produce Arrow RecordBatches directly from a stream of Netatmo GeoJSON messages.
    """
    df_stream = netatmo_dataframe_stream(
        message_stream,
        batch_size=batch_size,
        logger=logger,
    )
    async for batch in dataframes_to_record_batches(
        df_stream,
        schema=schema,
        preserve_index=preserve_index,
    ):
        yield batch
