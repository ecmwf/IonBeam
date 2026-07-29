Domain
======

This document explains the domain concepts and processing logic in IonBeam: how observations are partitioned into windows, how coverage is tracked, and how windows are built, revised, and sealed. For the system architecture, see :ref:`architecture:Architecture`.

Dataset Configuration
---------------------

A data source declares only its dataset name and data schema at ingestion (see :ref:`flight-interface:Ingest (DoPut)`). How the output dataset is built, presented, revised, and finalised is decided server-side, in the core service's dataset registry, keyed by dataset name. Datasets without a registry entry fall back to ``defaults``:

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
          finalize_after: PT48H

``aggregation_span``
  Duration of each aggregation window (ISO 8601 duration, e.g. ``PT1H``).

``rebuild_debounce``
  Trailing debounce for rebuilding an already-built window when late data arrives: each straggler defers the rebuild by this, so a wave coalesces into one rebuild shortly after it ends.

``finalize_after``
  How long after a window closes it becomes immutable. When unset, the window stays revisable for as long as the time-series database retains its observations.

``dedup_capacity``
  Expected distinct row contents per window, sizing the per-window deduplication filter.

Time Windows
------------

Observations are partitioned into fixed-duration time windows by each dataset's ``aggregation_span``. Two timestamps matter throughout:

Observation time
  When the measurement was recorded. This determines which window an observation belongs to.

Arrival time
  When IonBeam ingests the observation. This may be seconds or days after the observation time, due to network delays, processing, or backfilling.

Observation time assigns data to windows; arrival time decides when a window is worth building.

Window Boundaries
~~~~~~~~~~~~~~~~~

Windows are computed deterministically from the Unix epoch, so every replica and every ingestion run agrees on the same boundaries. An observation's window is found by truncating its timestamp to the nearest ``aggregation_span`` boundary; the window is ``[window_start, window_start + aggregation_span)``.

For ``aggregation_span: PT1H``, an observation at ``2024-01-15T14:23:45Z`` falls into ``[2024-01-15T14:00:00Z, 2024-01-15T15:00:00Z)``.

Window Lifecycle
~~~~~~~~~~~~~~~~

A window passes through three phases, each governing how late-arriving data is handled:

.. code-block:: text

              window closes        first build eligible               finalize floor
    ────────────────┬──────────────────────┬────────────────────────────────┬──────────▶ wall clock
      [12:00 … 13:00)                      │                                │
                    │◀──── settling ──────▶│◀───────── provisional ────────▶│   final
                    │ wait for the p95     │ late data rebuilds the window, │ sealed: stragglers stay
                    │ arrival lateness,    │ debounced by                   │ in InfluxDB, the dataset
                    │ measured per dataset │ rebuild_debounce               │ is never rewritten

Settling
  A freshly closed window waits before its first build for the *settle duration*: the measured 95th percentile of the dataset's arrival lateness. Ingestion records, for every datum, the gap between arrival time and observation time in a per-dataset histogram. The coordinator reads the percentile back and schedules the first build for ``window.end + settle``, so each source tunes its own wait. A dataset with no lateness history yet builds immediately and is revised during the provisional phase.

Provisional
  From its first build until the finalize floor, a window is revisable: late data folds in by rebuilding the window and re-publishing it to exporters. Rebuilds are debounced by ``rebuild_debounce``: each arriving record defers the rebuild, so a wave of stragglers or a backfill sweeping through historical windows coalesces into one rebuild shortly after the wave ends, rather than one per record.

Final
  Once ``now >= window.end + finalize_after``, a built window is sealed and a straggler can no longer rewrite it. The straggler's observations stay in the time-series database; they are simply not folded into the immutable window. When ``finalize_after`` is unset, the floor falls to the time-series database's retention period, since a window cannot be rebuilt from data that has aged out of the hot store anyway. A window that was never built still earns one final build past the floor.

Only rows whose full content is newly seen feed the lateness histogram: pull sources re-fetching an overlapping span deliver identical rows again, and counting those would inflate the percentile. Seen content is tracked in one Bloom filter per aggregation window, shared across replicas and expiring shortly after the window seals. The filter is keyed on a digest of the entire row, so a changed value, such as a QC update, counts as a genuine late arrival. Rows belonging to a window already past its finalize floor are skipped outright: they can no longer affect any build, so they must not push the settle estimate upward.

Coverage Claims
---------------

An ingestion call declares the temporal range it covers up front (``start``/``end`` on the :ref:`flight-interface:Ingest (DoPut)` descriptor), but the coordinator does not wait for the stream to finish. As batches are written, the ingestion handler tracks the **watermark**, the latest observation time written so far. Each time the watermark crosses an aggregation window boundary it publishes a **coverage claim** (an internal ``DataAvailableEvent``) making two distinct statements:

- the claim's **span** says "this range was swept" — it distinguishes data that is missing from data that does not exist, which observation cadence alone cannot
- the claim's **records** say "these windows received rows": one record per aggregation window the claim actually delivered observations into, each with the id every one of those rows was tagged with

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

Claim spans chain contiguously: each starts where the previous one ended, so coverage analysis never sees a false gap between them. Records exist only where rows landed — a swept-but-quiet window is covered by the claim's span and named by no record, so nothing downstream waits on rows that never existed. When the stream completes, a final claim covers the remaining tail through the declared ``end``, widened if data ran past it. Claim and record ids derive deterministically from the ingestion operation's ``id``, so a retried command re-publishes under identical ids and a replay is not mistaken for new data.

A bounded command whose data stays inside one aggregation window never crosses a boundary, so it publishes exactly one claim, spanning the declared range.

Data older than the claimed range (out-of-order within one stream) widens the next claim's start downward, and the late rows get records in their own windows. Those fresh record ids change the affected windows' desired record sets and force rebuilds. This is the same mechanism that handles late data arriving across separate ingestion operations.

If a stream fails mid-way, the claims already published stand: their observations are in the database and the coordinator knows it. The tail is never claimed, so no window is built from data the failure cut short. The source retries the command, and the deterministic ids make the replay idempotent.

Streaming ingestion
~~~~~~~~~~~~~~~~~~~

Because windows build on claims rather than on stream completion, a single long-running ``DoPut`` produces datasets while it is still streaming:

.. code-block:: text

    One continuous DoPut declaring [10:00 - 22:00), aggregation_span=PT1H, settle ≈ 30 min

    observation time  ─────▶ 10:59 ┃ 11:00 ────────▶ 12:01 ┃ ────▶
                                   ▼                      ▼
    claims                 claim 1 [10:00 ── 11:02]  claim 2 [11:02 ── 12:01]

    window [10:00-11:00)   coverage complete at claim 1
                           scheduled, eligible at 11:00 + settle ≈ 11:30
                           claimed by a builder at 11:30, built, published
    window [11:00-12:00)   coverage complete at claim 2, eligible ≈ 12:30, ...

Each window's dataset lands roughly the aggregation span plus the settle duration behind the live data, for as long as the stream stays open.

Out-of-Order Processing
-----------------------

Observations may arrive in any order: real-time streams can deliver historical backfills, and sources may publish with processing delays. The coordinator keeps every claim (for coverage analysis) and every record (as the audit log of who delivered which window's rows). For every claim it receives, the coordinator:

1. Folds each of the claim's records into that window's desired record set
2. Re-analyses the dataset's coverage from the stored claim spans
3. For every window the claim spans, compares the desired set against the last-built state to decide whether to schedule a build

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

The hash is computed over record ids, not over the observation data itself. If a new record covers the same data as a previous one, its fresh UUID still changes the hash and a rebuild is scheduled; the build's fold collapses the overlap (see :ref:`domain:Duplicate and Corrected Observations`). Because desired sets hold only records that delivered rows, a window's hash names exactly the row batches its build composes. The build queue keeps at most one entry per window, so repeated triggers for the same window coalesce into a single build.

Window Readiness
~~~~~~~~~~~~~~~~

The coordinator only schedules windows whose claimed coverage can support a build, which prevents publishing partial datasets. For a provisional window it validates:

- **Coverage**: claim spans fully cover ``[window.start, window.end)``
- **Gaps**: no temporal gaps exist between consecutive claim boundaries. Missing observations *within* a claim are by definition data that does not exist — the claim says the range was swept.
- **Records**: at least one record delivered rows into the window; a covered window with no records holds no data and has nothing to build.

Coverage and gap failures resolve only when new data arrives, and the claim carrying that data spans the affected windows and re-decides them.

A window that passes is scheduled with an **eligibility time**, the moment it becomes worth building, and the queue holds it until then:

- A first build is eligible at ``window.end + settle``
- A rebuild is eligible at ``arrival + rebuild_debounce``, re-decided on every arrival

A later claim that changes the window's desired set simply reschedules it; the queue keeps one entry per window. Because the delay lives in the queue rather than in a coordinator-side holding pen, a scheduled window builds when its time comes even if its source goes quiet. No later claim is needed to release it.

A never-built window past its finalize floor skips the coverage gates and earns one final build, eligible immediately, from whatever data arrived.

Duplicate and Corrected Observations
------------------------------------

Every stored row carries its record's id as a tag, so the time-series database preserves each record's delivery rather than upserting across them: history accumulates per record, and a build selects exactly the record set it wants. The consequence is that nothing in the write path collapses observations — that is the **build fold**'s job, and it is unconditional. A build reads its desired records' rows, groups them by observation identity (tag values and observation time), and keeps each identity's row from the latest-arrived record. Last claim wins, deterministically, over an explicit record set.

Two mechanisms keep the volume feeding that fold proportionate:

- Within one record, InfluxDB still upserts on (tags, record id, time) — a batch redelivering a row under the same record id overwrites itself.
- Across records, datasets with ``dedup_ingestion`` enabled fingerprint every row against a per-window filter and store only content not already stored — a sweep source re-fetching six days of history writes just the novel slice. This is an efficiency knob, not a correctness one: redelivered content that does reach the store is identical under the fold and collapses at build time.

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

A correction reaches published datasets through the ordinary rebuild path: the ingestion operation that carried it publishes claims like any other, the fresh record ids change the desired hash of every window they landed rows in, and a revisable window rebuilds through the fold. A window past its finalize floor keeps its published values; the correction stays queryable in the time-series database but is not folded into the sealed dataset.

Dataset Builder
---------------

Builders are workers that lease due windows from the shared build queue and materialise them as Arrow datasets. Every builder replica polls the same queue. The atomic lease guarantees a window is built by one replica at a time, and a lease left behind by a crashed builder expires and returns the window to the queue (see :ref:`architecture:Scaling`).

Build Order
~~~~~~~~~~~

The queue is a sorted set scored by each window's eligibility time. A claim takes the earliest-eligible window whose time has passed; windows scheduled for the future are invisible to builders until they come due. Since a first build's eligibility is ``window.end + settle``, historical backlogs are processed before recent windows.

Build Process
~~~~~~~~~~~~~

For each leased window:

1. Retrieve the desired record set and check ``desired_hash`` against ``observed_hash``; release the lease if they already match
2. Query InfluxDB for exactly the desired records' rows in ``[window.start, window.end)``, streamed batch-by-batch without a server-side sort, keeping heavy work out of the database
3. Verify every desired record's rows were reachable — anything less means the hot store lost or expired data, and the build defers rather than publish a partial record set
4. Fold: collapse to one row per observation identity, the latest-arrived record winning
5. Sort by time in the builder's own memory, convert to Arrow RecordBatches matching the canonical schema, and write to the arrow store under the window's deterministic key
6. Append the build to the window's manifest (see :ref:`domain:Window Manifests`)
7. Publish a ``DataSetAvailableEvent`` with the dataset location, marked final when the window is past its finalize floor
8. Update ``observed_hash`` and release the lease

Every published build passes through the fold — there is no other path to the canonical store. A window with no desired records has nothing to build and settles without publishing.

If the build fails (database timeout, storage error, an unreachable record set), the window is rescheduled and retried with exponential backoff; after repeated failures it is dropped and picked up again by the next claim that changes its content.

Window Manifests
~~~~~~~~~~~~~~~~

Coordination state expires with the hot period, so each build also records durable provenance twice: its own entry is embedded in every build file's Parquet footer (schema metadata key ``ionbeam.build``), and the window's full build history lives under the dataset's ``_manifests/`` prefix (underscore-prefixed so standard dataset discovery skips it), each entry naming its build's exact file set. The manifest holds the window's identity, the declared schema of the latest build, and one entry per build: version, build time, record-set hash, schema hash, composition mode, row count, finality, software version, and the ingestion records folded in with their claimed spans and arrival times.

The manifest is written after the build's files. A crash between the writes leaves the manifest one build behind; the next rebuild rewrites both.

Each newly registered schema is logged the same way, as an immutable document at ``registrations/<dataset>/<schema hash>.json``.

Concurrency
~~~~~~~~~~~

The ``concurrency`` setting controls how many windows build in parallel within one builder replica. Each build operates independently with its own InfluxDB query stream and arrow store writer. Total build parallelism is ``concurrency × replicas``, since every replica leases from the same queue.
