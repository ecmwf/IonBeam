Domain
===============

This document explains the core domain concepts and processing logic in Ionbeam. For an overview of the system architecture and message flow, see :ref:`architecture:Architecture`.

Dataset Configuration
---------------------

Each dataset is configured through metadata provided by data sources in every :ref:`messaging-interface:IngestDataCommand` message. The :ref:`messaging-interface:DatasetMetadata` structure defines how the system processes observations for that dataset:

.. code-block:: json

    {
      "metadata": {
        "dataset": {
          "name": "weather_stations",
          "aggregation_span": "PT1H",
          "subject_to_change_window": "PT6H",
          "description": "Ground weather station observations",
          "source_links": [],
          "keywords": ["weather", "temperature"]
        }
      }
    }

**Key Configuration Parameters:**

- ``aggregation_span``: Time window duration for dataset aggregation (ISO 8601 duration, e.g., "PT1H" for 1 hour)
- ``subject_to_change_window``: Grace period after window close during which late data is accepted (ISO 8601 duration)
- ``name``: Unique dataset identifier used for organizing storage and routing

These parameters control the temporal partitioning, late-arrival handling, and export behavior described in the following sections.

Time Windows
------------

Observations are partitioned into fixed-duration time windows based on each dataset's ``aggregation_span`` parameter. This determines the temporal granularity at which data is aggregated and exported.

**Terminology:**

- **Observation Time**: The timestamp when the measurement was recorded (e.g., when a weather station recorded temperature at ``2024-01-15T13:30:00Z``). This determines which aggregation window the data belongs to.
- **Arrival Time**: The timestamp when Ionbeam receives and ingests the data. This may be seconds, minutes, or hours after the observation time due to network delays, processing, or backfilling.

The system uses observation time to assign data to windows, but uses arrival time to determine whether the window should be built or continue waiting for additional data.

Window Alignment
~~~~~~~~~~~~~~~~

All windows align to Unix epoch (1970-01-01T00:00:00Z) to ensure consistency across different ingestion runs. Given an observation timestamp, the window boundaries are computed by:

1. Calculate seconds since epoch
2. Truncate to the nearest ``aggregation_span`` boundary
3. Define window as ``[window_start, window_start + aggregation_span)``

For a dataset with ``aggregation_span: PT1H`` (1 hour), an observation at ``2024-01-15T14:23:45Z`` falls into the window ``[2024-01-15T14:00:00Z, 2024-01-15T15:00:00Z)``.

Late-Arriving Data
~~~~~~~~~~~~~~~~~~

The ``subject_to_change_window`` parameter controls how long after a window closes the system will accept new data and delay window builds. This accommodates data sources with processing delays or backfill operations.

For a window ``[2024-01-15T13:00:00Z, 2024-01-15T14:00:00Z)`` with ``subject_to_change_window: "PT6H"``:

- Observations with timestamps in ``[13:00, 14:00)`` can arrive (be ingested) up until ``2024-01-15T20:00:00Z`` (window end + 6 hours)
- The window will not be built until after ``2024-01-15T20:00:00Z`` (current time must exceed window end + STC window)
- Data arriving before ``2024-01-15T20:00:00Z`` triggers a rebuild if the window was already built
- Data arriving after ``2024-01-15T20:00:00Z`` is ignored (window finalized)

Set ``subject_to_change_window: "PT0S"`` for real-time sources that never deliver late data and should build immediately after the window closes.

Out-of-Order Processing
------------------------

Observations may arrive in any order—real-time streams can deliver historical backfills, or sources may publish data with processing delays. The coordinator maintains an audit log of every ingestion event (stored in Redis) with its temporal bounds. When a new ingestion completes, the coordinator:

1. Identifies all windows that overlap the ingestion's time range
2. For each window, computes which ingestion events have contributed data
3. Compares this against the last-built state to determine if a rebuild is needed

This approach handles arbitrary arrival patterns:

.. code-block:: text

    Time →
    ──────────────────────────────────────────
    Window A    Window B    Window C
    [00-01)     [01-02)     [02-03)

    Ingestion 1: [00:30 - 01:30]  → affects A, B
    Ingestion 2: [01:45 - 02:15]  → affects B, C
    Ingestion 3: [00:15 - 00:45]  → affects A

Each ingestion event is assigned a UUID. The coordinator computes the **desired event set** for each window by taking the union of all overlapping ingestion IDs.

Window Rebuild Logic
~~~~~~~~~~~~~~~~~~~~

For each affected window:

1. Compute ``desired_events = {all ingestion IDs overlapping this window}``
2. Hash the sorted ID list: ``desired_hash = sha256(sorted(desired_events))``
3. Retrieve ``observed_hash`` from Redis (hash when last built)
4. If ``desired_hash != observed_hash``, enqueue window for rebuild

This deduplicates redundant rebuilds when the same data arrives multiple times.

Window Readiness
~~~~~~~~~~~~~~~~

By default, the coordinator only enqueues windows for building when they meet completeness criteria. This prevents exporting partial datasets that may be missing data. The coordinator validates:

- **STC Cutoff**: Current time must be past the subject-to-change window (``now >= window.end + subject_to_change_window``)
- **Coverage**: Ingestion events must fully span the window boundaries ``[window.start, window.end)``
- **Gaps**: No temporal gaps exist between consecutive ingestion event boundaries. Missing observations within an event are acceptable—the system cannot distinguish true gaps from expected sparse data.


.. note::

  The coordinator handler's ``allow_incomplete_windows`` configuration parameter can be set to ``true`` to disable these validation checks:

  .. code-block:: yaml

      dataset_coordinator:
        allow_incomplete_windows: true

**Default:** ``false`` (validation enabled)

Dataset Builder
---------------

The builders runs as worker that dequeues windows from a shared priority queue and materializes them as Arrow datasets.

Build Priority
~~~~~~~~~~~~~~

Windows are prioritized by age—oldest windows build first. Priority is set as ``-int(window.end.timestamp())``, so:

- Window ending ``2024-01-15T12:00:00Z`` gets priority ``-1705320000``
- Window ending ``2024-01-15T13:00:00Z`` gets priority ``-1705323600``

The queue pops highest priority first. This ensures historical backlogs are processed before recent windows.

Build Process
~~~~~~~~~~~~~

For each dequeued window:

1. Retrieve the desired event set (union of all overlapping ingestion IDs)
2. Check if ``desired_hash == observed_hash`` (skip if already built)
3. Query InfluxDB for all observations in ``[window.start, window.end)``
4. Stream results in 15-minute slices to avoid buffering large windows
5. Convert each slice to Arrow RecordBatches matching the canonical schema
6. Write batches incrementally to object storage as Arrow IPC
7. Publish ``DataSetAvailableEvent`` with the dataset location
8. Update ``observed_hash`` to mark this window as current

If the build fails (database timeout, storage error), the window is re-enqueued with the same priority for automatic retry.

Concurrency
~~~~~~~~~~~

The ``concurrency`` setting controls how many windows build in parallel. Each build operates independently with its own InfluxDB query stream and object storage writer. Set ``concurrency: 1`` for single-threaded processing or higher values to parallelize builds across different windows.
