# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Parquet writing policy for built dataset files.

Every persisted dataset window — the canonical object-store build and the
pygeoapi mirror — goes through :class:`BufferedParquetWriter`, so file layout
is a deliberate policy rather than an accident of the producer's batch sizes:

* row groups of exactly ``row_group_rows`` (incoming batches are buffered and
  re-sliced, so many small batches never become many small row groups),
* zstd-compressed pages with the page index, giving readers page-level
  min/max pruning on top of row-group statistics,
* the time ordering the builder already paid for declared via
  ``sorting_columns`` so readers can exploit it.
"""

import pyarrow as pa
import pyarrow.parquet as pq

# ~5–25 MB uncompressed per group at canonical dataset column widths.
DEFAULT_ROW_GROUP_ROWS = 131_072


class BufferedParquetWriter:
    """Write RecordBatches to one Parquet file with uniform row groups.

    ``sorted_by`` names a column the incoming stream is already ordered by;
    it is declared in the file metadata, never sorted here.
    """

    def __init__(
        self,
        where,
        schema: pa.Schema,
        sorted_by: str | None = None,
        row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
    ):
        sorting = None
        if sorted_by is not None:
            # names.index raises on a missing column; get_field_index would
            # silently return -1 and write nonsense sort metadata.
            sorting = [pq.SortingColumn(schema.names.index(sorted_by))]
        self._schema = schema
        self._row_group_rows = row_group_rows
        self._buffer: list[pa.RecordBatch] = []
        self._buffered_rows = 0
        self._writer = pq.ParquetWriter(
            where,
            schema,
            compression="zstd",
            compression_level=3,
            write_page_index=True,
            sorting_columns=sorting,
        )

    def write_batch(self, batch: pa.RecordBatch) -> None:
        if batch.num_rows == 0:
            return
        self._buffer.append(batch)
        self._buffered_rows += batch.num_rows
        if self._buffered_rows >= self._row_group_rows:
            self._flush()

    def _flush(self) -> None:
        """Write all full row groups; carry the remainder so groups stay uniform."""
        table = pa.Table.from_batches(self._buffer, schema=self._schema)
        full = (table.num_rows // self._row_group_rows) * self._row_group_rows
        self._writer.write_table(
            table.slice(0, full), row_group_size=self._row_group_rows
        )
        tail = table.slice(full)
        self._buffer = tail.to_batches()
        self._buffered_rows = tail.num_rows

    def close(self) -> None:
        if self._buffered_rows:
            table = pa.Table.from_batches(self._buffer, schema=self._schema)
            self._writer.write_table(table)
            self._buffer.clear()
            self._buffered_rows = 0
        self._writer.close()
