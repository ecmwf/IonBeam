

import asyncio
import pathlib
from typing import AsyncIterator, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

logger = structlog.get_logger(__name__)

# TODO implement parquet stream reader

async def stream_dataframes_to_parquet(
    dataframe_stream: AsyncIterator[pd.DataFrame],
    output_path: pathlib.Path,
    schema_fields: Optional[List[Tuple[str, pa.DataType]]] = None
) -> int:
    """
    Stream pandas DataFrames to a parquet file.
    
    Args:
        dataframe_stream: Async iterator of pandas DataFrames
        output_path: Path where parquet file will be written
        schema_fields: Optional parquet schema. If None, it will be inferred from the
                       first non-empty dataframe and applied to all subsequent batches.
    """
    schema = pa.schema(schema_fields) if schema_fields else None
    total_rows = 0
    chunk_count = 0
    writer: Optional[pq.ParquetWriter] = None
    tmp_path = output_path.with_suffix(output_path.suffix + '.tmp')

    try:
        async for data in dataframe_stream:
            await asyncio.sleep(0) # Keep background tasks (connection to broker etc..) ticking over
            if data is None or data.empty:
                continue

            # Lazily initialize schema/writer from first non-empty batch if not provided
            if writer is None:
                if schema is None:
                    # Infer schema from the first batch
                    table = pa.Table.from_pandas(data, preserve_index=False)
                    schema = table.schema
                writer = pq.ParquetWriter(tmp_path, schema=schema)

            # Ensure all schema columns exist, fill missing ones with defaults
            for col_name, col_type in zip(schema.names, schema.types):
                if col_name not in data.columns:
                    data[col_name] = "" if pa.types.is_string(col_type) else np.nan

            # Select only schema columns in correct order
            data = data[schema.names]

            # Convert to PyArrow RecordBatch and write
            try:
                batch = pa.RecordBatch.from_pandas(
                    data,
                    schema=schema,
                    preserve_index=False
                )
                writer.write_batch(batch)
                total_rows += len(data)
                chunk_count += 1

                if chunk_count % 100 == 0:
                    logger.info("Parquet write progress", chunks=chunk_count, rows=total_rows)

            except Exception as e:
                logger.exception("Failed to write batch", error=str(e))
                raise
    finally:
        if writer is not None:
            writer.close()

        tmp_path.replace(output_path)

    return total_rows

