

import logging
import pathlib
from typing import AsyncIterator, List, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# TODO implement parquet stream reader

async def stream_dataframes_to_parquet(
    dataframe_stream: AsyncIterator[pd.DataFrame],
    output_path: pathlib.Path,
    schema_fields: List[Tuple[str, pa.DataType]]
) -> int:
    """
    Stream pandas DataFrames to a parquet file.
    
    Args:
        dataframe_stream: Async iterator of pandas DataFrames
        output_path: Path where parquet file will be written
        schema_fields: parquet schema structure - used to ensure all we don't drop fields by only using the cols from the first batch
    """
    schema = pa.schema(schema_fields)
    total_rows = 0
    chunk_count = 0
    
    with pq.ParquetWriter(output_path, schema=schema) as writer:
        async for data in dataframe_stream:
            if data is None or data.empty:
                continue
                
            # Ensure all schema columns exist, fill missing ones with defaults
            for col_name, col_type in zip(schema.names, schema.types):
                if col_name not in data.columns:
                    # logger.warning("Not found %s when creating parquet schema", col_name)
                    # logger.warning(data.head())
                    data[col_name] = "" if pa.types.is_string(col_type) else np.nan
            
            # Select only schema columns in correct order
            data = data[schema.names]
            
            # Convert to PyArrow RecordBatch
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
                    logger.info(f"Processed {chunk_count} chunks, {total_rows} rows so far")
                    
            except Exception as e:
                logger.error(f"Failed to write batch: {e}")
                raise
    
    return total_rows

