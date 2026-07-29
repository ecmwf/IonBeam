# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Content fingerprints for redelivery suppression: a digest of a row's full
canonical content — identity *and* values — so a re-delivered row reads as
seen while a genuine change (a QC update) reads as new. The record store
remembers them per (dataset, aggregation window) until the window seals."""

import hashlib

import pyarrow as pa
import pyarrow.compute as pc


def row_fingerprints(table: pa.Table) -> list[bytes]:
    """A 16-byte digest per row, deterministic over the row's full canonical
    content: identical rows yield identical digests, any changed value yields a
    different one. Columns are string-cast by Arrow (stable formatting for a
    given canonicalization) and joined per row. The digests are a per-row Python
    loop — CPU-bound, so event-loop callers should run this in a worker thread.
    """
    parts = [
        table.column(name).cast(pa.string()).combine_chunks()
        for name in table.column_names
    ]
    # \x00 cannot occur in a canonical string value, so a null column never
    # collides with an empty one.
    joined = pc.binary_join_element_wise(
        *parts, "\x1f", null_handling="replace", null_replacement="\x00"
    )
    return [
        hashlib.blake2b(row.encode(), digest_size=16).digest()
        for row in joined.to_pylist()
    ]
