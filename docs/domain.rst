Domain
======

This document explains the domain concepts and processing logic in IonBeam: how observations are partitioned into windows, how coverage is tracked, and how windows are built, revised, and sealed. For the system architecture, see :ref:`architecture:Architecture`.

Dataset Configuration
---------------------

A data source declares only its dataset name and schema at ingestion (see :ref:`flight-interface:Ingest (DoPut)`). The core service controls how the dataset is built, presented, revised, and finalised. These settings are stored in the dataset registry and keyed by dataset name. A dataset without a registry entry uses ``defaults``:

.. code-block:: yaml

    datasets:
      defaults:
        aggregation_span: PT1H
        rebuild_debounce: PT0S
      registry:
        acronet:
          description: "IoT observations from the CIMA Acronet network."
          aggregation_span: P1D
          rebuild_debounce: PT10M

``aggregation_span``
  Duration of each aggregation window (ISO 8601 duration, e.g. ``PT1H``).

``rebuild_debounce``
  Trailing debounce for rebuilding an already-built window when late data arrives: each late arrival defers the rebuild by this, so a wave coalesces into one rebuild shortly after it ends.

Time Windows
------------

Each dataset's ``aggregation_span`` divides observations into fixed-duration time windows. Two timestamps determine how an observation is processed:

Observation time
  When the measurement was recorded. This determines which window an observation belongs to.

Arrival time
  When IonBeam ingests the observation. This may be seconds or days after the observation time, due to network delays, processing, or backfilling.

Observation time assigns data to a window. Arrival time contributes to the decision about when that window should be built.

Window Boundaries
~~~~~~~~~~~~~~~~~

Window boundaries are aligned to the Unix epoch and therefore do not depend on a particular replica or ingestion run. Truncating an observation timestamp to the preceding ``aggregation_span`` boundary gives ``window_start``. The resulting half-open interval is ``[window_start, window_start + aggregation_span)``.

For ``aggregation_span: PT1H``, an observation at ``2024-01-15T14:23:45Z`` falls into ``[2024-01-15T14:00:00Z, 2024-01-15T15:00:00Z)``.

Window Lifecycle
~~~~~~~~~~~~~~~~

A window passes through three phases. These phases balance prompt publication against the need to incorporate late data and eventually produce an immutable result.

.. code-block:: text

              window closes        first build eligible               retention floor
    ────────────────┬──────────────────────┬────────────────────────────────┬──────────▶ wall clock
      [12:00 … 13:00)                      │                                │
                    │◀──── settling ──────▶│◀───────── provisional ────────▶│   final
                    │ wait for the p95     │ late data rebuilds the window, │ sealed: late data stays
                    │ arrival lateness,    │ debounced by                   │ in InfluxDB, the dataset
                    │ measured per dataset │ rebuild_debounce               │ is never rewritten

Settling
  After a window closes, its first build waits for the *settle duration*. This duration is the measured 95th percentile of arrival lateness for the dataset. Ingestion records the difference between arrival time and observation time in a per-dataset histogram, and the coordinator schedules the first build for ``window.end + settle``. A dataset without lateness history has a settle duration of zero and may be revised during the provisional phase.

Provisional
  From its first build until the retention limit, a window remains revisable. Late data schedules another build and a new notification to exporters. The ``rebuild_debounce`` setting delays each revision after the most recent arrival, allowing a group of late records or a backfill to be handled by one rebuild.

Final
  A window becomes final when its end is older than the hot-store retention period. Later observations may remain in the time-series database until they expire, but they do not change the published dataset. This limit is necessary because a complete rebuild is no longer possible after source observations expire from the hot store.

The lateness histogram counts rows written to the store. For a dataset with ``dedup_ingestion`` enabled, an identical redelivery is filtered before it can affect the histogram. A changed value, such as a quality-control update, is new content and contributes a new lateness measurement. Rows for final windows are not written and therefore do not affect the estimate.

Coverage Claims
---------------

An ingestion call declares its temporal range in the ``start`` and ``end`` fields of the :ref:`flight-interface:Ingest (DoPut)` descriptor. The coordinator can process coverage before the stream finishes. As batches are written, the ingestion handler tracks a **watermark**: the latest observation time written so far. Whenever the watermark crosses an aggregation-window boundary, the handler publishes a **coverage claim** (an internal ``DataAvailableEvent``).

A coverage claim contains two kinds of information:

- The claim's **span** records the range checked by the source. It distinguishes an empty interval from one the source has not checked.
- The claim's **records** identify the windows that received rows. Each record contains the identifier attached to those rows.

.. code-block:: text

    DoPut declares [10:00 - 13:00), aggregation_span=PT1H

    watermark:    10:07 .. 10:58 │ 11:03 .. 11:41 │ 12:02 ..   stream ends
                                 ▼                ▼            ▼
    claim 1                [10:00 ──── 11:03]     │            │
      records                {10:00, 11:00}       │            │
    claim 2                            [11:03 ── 12:02]        │
      records                            {11:00}               │
    final claim                                   [12:02 ── 13:00]
      records                                       {12:00}

Successive claim spans are contiguous: each begins where the previous span ended. This prevents coverage analysis from interpreting a batch boundary as a gap. A checked interval with no observations appears in the span but has no record, so downstream processing does not wait for rows that the source did not find.

When the stream completes, a final claim extends coverage through the declared ``end``. If the stream contains later observations, the final span is extended to include them. Claim and record identifiers are derived from the ingestion operation's ``id``. Retrying the same operation therefore republishes the same identifiers.

A bounded command whose data stays inside one aggregation window never crosses a boundary, so it publishes exactly one claim, spanning the declared range.

If a stream contains observations older than its current claimed range, the next claim starts early enough to include them. The corresponding windows receive new records, which changes their desired record sets and schedules revisions where permitted. The same mechanism handles late data delivered by separate ingestion operations.

If a stream fails, observations and claims from completed batches remain valid. The unprocessed part of the declared range is not claimed, so it cannot make a window appear complete. The source can retry with the same ingestion identifier without creating distinct claim identities.

Streaming ingestion
~~~~~~~~~~~~~~~~~~~

Windows become eligible from coverage claims rather than from stream completion. A long-running ``DoPut`` can therefore produce completed windows before the stream closes:

.. code-block:: text

    One continuous DoPut declaring [10:00 - 22:00), aggregation_span=PT1H, settle ≈ 30 min

    observation time  ─────▶ 10:59 ┃ 11:00 ────────▶ 12:01 ┃ ────▶
                                   ▼                      ▼
    claims                 claim 1 [10:00 ── 11:02]  claim 2 [11:02 ── 12:01]

    window [10:00-11:00)   coverage complete at claim 1
                           scheduled, eligible at 11:00 + settle ≈ 11:30
                           claimed by a builder at 11:30, built, published
    window [11:00-12:00)   coverage complete at claim 2, eligible ≈ 12:30, ...

While the stream remains open, each window can be published after its end plus the settle duration.

Out-of-Order Processing
-----------------------

Observations do not need to arrive in chronological order. A real-time stream may include a historical backfill, and a source may publish observations after processing delays. The coordinator retains claims for coverage analysis and records for the ingestion audit trail. For each claim, it:

1. Folds each of the claim's records into that window's desired record set
2. Re-analyses the dataset's coverage from the stored claim spans
3. Compares each affected window's desired set with its last-built state and schedules a build when they differ

The example below shows three ingestion operations touching one window:

.. code-block:: text

    window [12:00 ─────────────── 13:00)          aggregation_span=PT1H

    ingest A   [12:00 ── 12:30]                   desired = {A}       coverage gap 12:30-13:00 → skip
    ingest B             [12:30 ── 13:00]         desired = {A,B}     complete → scheduled,
                                                                      built at end + settle, published
    ingest C                 [12:45 ──── 13:15]   desired = {A,B,C}   hash changed → rescheduled,
                                                                      rebuilt, re-published
                                                  (C's claim also spans [13:00-14:00);
                                                   that window only gains a record — and a
                                                   rebuild — if C delivered rows into it)

Window Rebuild Logic
~~~~~~~~~~~~~~~~~~~~

For each record a claim carries:

1. Fold the record's UUID into its window's ``desired_records`` set (a server-side set union, so concurrent coordinator replicas never lose each other's records)
2. Hash the sorted id list: ``desired_hash = sha256(sorted(desired_records))``
3. Retrieve ``observed_hash``, the hash when last built
4. If ``desired_hash != observed_hash``, schedule the window for building

The hash represents record identifiers rather than observation values. A new record changes the hash even when it overlaps data from an earlier record, so the window is scheduled again. During the build, the fold resolves that overlap (see :ref:`domain:Duplicate and Corrected Observations`).

The desired set contains only records that delivered rows to the window. Its hash therefore identifies the inputs selected for the build. The queue holds at most one entry for each window, so repeated scheduling requests are combined.

Window Readiness
~~~~~~~~~~~~~~~~

The coordinator schedules a window only when its claimed coverage supports a complete build. For a provisional window, it validates:

- **Coverage**: claim spans fully cover ``[window.start, window.end)``
- **Gaps**: no temporal gaps exist between consecutive claim boundaries. An interval inside a claim may contain no observations; the claim still records that the source checked it.
- **Records**: at least one record delivered rows into the window; a covered window with no records holds no data and has nothing to build.

An incomplete window is reconsidered when a later claim changes its coverage.

A window that passes these checks receives an **eligibility time**. The queue retains the window until that time:

- A first build is eligible at ``window.end + settle``
- A rebuild is eligible at ``arrival + rebuild_debounce``, re-decided on every arrival

A later claim that changes a window's desired set updates its queue entry. Eligibility time is stored with that entry, so no subsequent claim is required when the time arrives.

Duplicate and Corrected Observations
------------------------------------

Each stored row carries its record identifier as a tag. The time-series database therefore preserves deliveries from separate records instead of upserting across them. A build selects its desired record set and applies the **build fold** to resolve overlapping observations.

The fold groups rows by observation identity, defined by tag values and observation time. For each identity, it retains the row from the record that arrived last. This rule is applied to every published build.

Two mechanisms limit the volume processed by the fold:

- Within one record, InfluxDB upserts on ``(tags, record id, time)``. Redelivering a row under the same record identifier overwrites that row.
- Across records, ``dedup_ingestion`` fingerprints rows with a per-window filter and stores only content that has not been seen. This is an optimisation rather than a correctness requirement; the build fold also resolves identical content that reaches the store under different records.

.. list-table::
   :header-rows: 1
   :widths: 28 34 38

   * - Arrival
     - In InfluxDB
     - In published datasets
   * - First arrival of a row
     - Row stored under its record's tag
     - Folded in when the window builds
   * - Identical redelivery (a pull source re-fetching an overlapping span)
     - Suppressed by the content filter, or stored under the fresh record's tag
     - The fresh record forces a rebuild; the fold keeps one row per identity
   * - Corrected value for an existing identity (e.g. a QC update)
     - Stored under the correcting record's tag; earlier deliveries kept
     - A revisable window rebuilds and the later-arrived record's row wins; a sealed window keeps its published values

A correction uses the normal revision path. Its ingestion operation publishes claims, and the new record identifiers change the desired hash of each affected window. Revisable windows are rebuilt and the fold selects the corrected rows. Final windows retain their published values; the correction remains in the time-series database until retention removes it.

Dataset Builder
---------------

Builders lease due windows from the shared build queue and materialise them as Arrow datasets. Every builder replica polls the same queue. An atomic lease assigns a window to one replica at a time. If that replica stops before completing the build, the lease expires and the window returns to the queue (see :ref:`architecture:Scaling`).

Build Order
~~~~~~~~~~~

The queue is a sorted set ordered by eligibility time. A builder claims the earliest eligible window whose time has passed. Future windows remain unavailable until their eligibility time. Since first builds use ``window.end + settle``, older eligible windows are processed before newer ones.

Build Process
~~~~~~~~~~~~~

For each leased window:

1. Retrieve the desired record set and compare ``desired_hash`` with ``observed_hash``. If they match, release the lease without rebuilding.
2. Query InfluxDB for the desired records in ``[window.start, window.end)``. Results are streamed in batches without a server-side sort.
3. Verify that every desired record is available. If data has expired or is otherwise unavailable, defer the build instead of publishing a partial result.
4. Apply the build fold, retaining the latest-arriving row for each observation identity.
5. Sort by time in the builder's own memory, convert to Arrow RecordBatches matching the canonical schema, and write to the arrow store under the window's deterministic key
6. Append the build to the window's manifest (see :ref:`domain:Window Manifests`)
7. Publish a ``DataSetAvailableEvent`` with the dataset location, marked final when the window is past its retention floor
8. Update ``observed_hash`` and release the lease

The fold is applied to every published build. A window with no desired records does not produce a dataset.

If a database timeout, storage error, or unavailable record set prevents a build, the window is rescheduled with exponential backoff. After repeated failures, it is removed from the queue. A later claim that changes the window's content schedules it again.

Window Manifests
~~~~~~~~~~~~~~~~

Coordination state expires with the hot-store retention period, so durable provenance is stored with each build. The build entry appears in the Parquet footer under the ``ionbeam.build`` schema metadata key. The window's full build history is stored under the dataset's ``_manifests/`` prefix, which standard dataset discovery ignores.

The manifest identifies the window and records the schema of its latest build. Each build entry includes its version, build time, record-set hash, schema hash, composition mode, row count, finality, software version, file set, and contributing ingestion records.

The manifest is written after the build's files. A crash between the writes leaves the manifest one build behind; the next rebuild rewrites both.

Each newly registered schema is logged the same way, as an immutable document at ``registrations/<dataset>/<schema hash>.json``.

Concurrency
~~~~~~~~~~~

The ``concurrency`` setting controls how many windows build in parallel within one builder replica. Each build operates independently with its own InfluxDB query stream and arrow store writer. Total build parallelism is ``concurrency × replicas``, since every replica leases from the same queue.
