# ECMWF ODB Exporter

Consumes built dataset windows from the ionbeam Flight endpoint and streams them as ODB-2 via codc, delivered as 6-hourly analysis-cycle files to the ECMWF store. A dataset event marks its cycle seen; once the cycle is past its data cutoff and has quiesced, the delivered `{dataset}_{yyyymmdd}_{hh}.odb` is re-encoded whole from the current build of every window the cycle covers (one `dataset_range` lookup), so out-of-order windows, replays and revisions need no tracking. Output goes to a local directory or an S3 prefix.

Run it with `uv run ecmwf-exporter --config config.yaml` (or set `ECMWF_CONFIG_PATH`). `config.example.yaml` documents the configuration: the ionbeam Flight URL, the dataset filter, and the ODB exporter settings including output path, per-dataset report identity, and assembly timing.
