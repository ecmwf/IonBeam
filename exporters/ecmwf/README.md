# ECMWF ODB Exporter

Retrieves built datasets from the IonBeam Flight endpoint and encodes them as ODB-2 with codc. Output is organised into six-hour analysis-cycle files and written to a local directory or an S3 prefix.

Each dataset event marks its analysis cycle as active. After the cycle passes its data cutoff and remains inactive for the configured period, the exporter reconstructs `{dataset}_{yyyymmdd}_{hh}.odb` from the current build of every window in that cycle. A single `dataset_range` request resolves those builds. Reconstructing the complete file incorporates out-of-order windows, redeliveries, and revisions without maintaining separate state for each case.

Run it with `uv run ecmwf-exporter --config config.yaml` (or set `ECMWF_CONFIG_PATH`). `config.example.yaml` documents the configuration: the ionbeam Flight URL, the dataset filter, and the ODB exporter settings including output path, per-dataset report identity, and assembly timing.
