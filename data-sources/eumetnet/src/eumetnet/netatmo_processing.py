# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import asyncio
import time
from collections.abc import AsyncIterable, Iterable
from typing import AsyncIterator, List, Optional

import pandas as pd
import pyarrow as pa
import structlog

from ionbeam_client.canonical_stream import canonical_record_batches
from ionbeam_client.alignment import drop_undeclared_columns

from .netatmo_metadata import PARAMETER_COLUMNS
from ionbeam_client.models import IngestionMetadata

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
    messages: List[dict], logger: Optional[structlog.stdlib.BoundLogger] = None
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

            # netatmo publishes precipitation with an inconsistent accumulation
            # period (-PT1M, PT0S, PT17H…, drifting per message) though the
            # values are the same reading — so collapse precip to one canonical
            # period, otherwise every variant lands in its own column and gets
            # dropped as undeclared. Other variables have stable periods.
            period = props.get("period")
            if standard_name == "precipitation_amount":
                period = "PT1M"

            parts = [
                standard_name,
                props.get("level"),
                props.get("function"),
                period,
            ]
            parameter = ":".join(str(p) for p in parts if p not in (None, ""))

            records.append(
                {
                    "_row_idx": idx,  # arrival order: the tie-break on equal pubtime
                    "station_id": station_id,
                    "datetime": dt_raw,
                    "lat": lat_raw,
                    "lon": lon_raw,
                    "pubtime": props.get("pubtime"),
                    "parameter": parameter,
                    "value": content.get("value"),
                    "qc_code": props.get("quality_code"),
                }
            )
        except Exception:
            invalid_count += 1
            continue

    if not records:
        return pd.DataFrame(columns=["station_id", "datetime", "lat", "lon"])

    df = pd.DataFrame.from_records(records)
    df["datetime"] = pd.to_datetime(
        df["datetime"], utc=True, format="ISO8601", errors="coerce"
    )
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["pubtime"] = pd.to_datetime(
        df["pubtime"], utc=True, format="ISO8601", errors="coerce"
    )
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["qc_code"] = pd.to_numeric(df["qc_code"], errors="coerce")

    before = len(df)
    df = df.dropna(subset=["station_id", "datetime", "lat", "lon"])
    dropped = before - len(df)
    if dropped or invalid_count:
        log.info(
            "Parsed netatmo records",
            parsed=before,
            dropped_after_coercion=dropped,
            invalid_during_parse=invalid_count,
        )

    df["station_id"] = df["station_id"].astype("category")
    df["parameter"] = df["parameter"].astype("category")

    key = ["station_id", "datetime", "lat", "lon", "parameter"]

    val_nunique = df.groupby(key, sort=False, observed=True)["value"].nunique(
        dropna=True
    )
    conflicts_count = int((val_nunique > 1).sum())
    if conflicts_count:
        log.warning(
            "Conflicting observations for identical keys; keeping latest by pubtime/arrival",
            conflicts=conflicts_count,
        )

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

    index = ["station_id", "datetime", "lat", "lon"]
    wide_values = dedup.pivot(index=index, columns="parameter", values="value")
    wide_qc = dedup.pivot(index=index, columns="parameter", values="qc_code")
    wide_qc.columns = [f"{col}_qc" for col in wide_qc.columns]
    wide = pd.concat([wide_values, wide_qc], axis=1).reset_index()
    for col in [col for col in wide.columns if col.endswith("_qc")]:
        wide[col] = wide[col].fillna(0).astype("int64")

    wide = wide.sort_values(["station_id", "datetime"]).reset_index(drop=True)
    wide.columns.name = None

    log.info(
        "netatmo batch processed",
        parsed=before,
        dropped=dropped,
        conflicts=conflicts_count,
        out_rows=len(wide),
        columns=wide.shape[1],
        elapsed_s=round(time.perf_counter() - t0, 2),
    )
    return wide


async def netatmo_dataframe_stream(
    message_stream: Iterable[dict] | AsyncIterable[dict],
    batch_size: int = 50000,
    logger: Optional[structlog.stdlib.BoundLogger] = None,
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

    def flush() -> pd.DataFrame:
        nonlocal total_msgs, total_batches
        df = process_netatmo_geojson_messages_to_df(buffer, log)
        total_batches += 1
        total_msgs += len(buffer)
        buffer.clear()
        return df

    async for msg in _iter_messages(message_stream):
        buffer.append(msg)
        if len(buffer) >= batch_size:
            df = flush()
            if not df.empty:
                yield df
    if buffer:
        df = flush()
        if not df.empty:
            yield df
    log.info(
        "Netatmo dataframe stream complete",
        batches=total_batches,
        total_input_msgs=total_msgs,
        elapsed=time.perf_counter() - stream_start,
    )


async def netatmo_record_batch_stream(
    message_stream: Iterable[dict] | AsyncIterable[dict],
    metadata: IngestionMetadata,
    *,
    batch_size: int = 50000,
    logger: Optional[structlog.stdlib.BoundLogger] = None,
) -> AsyncIterator[pa.RecordBatch]:
    """Produce aligned Arrow RecordBatches from a stream of Netatmo GeoJSON messages.

    The pivoted E-SOH parameter keys are renamed to their canonical columns here,
    and working columns beyond the declared schema are dropped — the source's
    explicit choice — before alignment.
    """
    df_stream = netatmo_dataframe_stream(
        message_stream,
        batch_size=batch_size,
        logger=logger,
    )

    async def declared_stream() -> AsyncIterator[pd.DataFrame]:
        async for df in df_stream:
            df = df.rename(columns=PARAMETER_COLUMNS)
            yield drop_undeclared_columns(df, metadata.dataset_schema)

    async for batch in canonical_record_batches(declared_stream(), metadata):
        yield batch
