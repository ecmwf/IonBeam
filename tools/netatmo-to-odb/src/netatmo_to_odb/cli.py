import json
import logging
import pathlib
import tarfile
import time
from collections.abc import Iterator

import click
import pyarrow as pa
import pyodc as odc
import structlog

from ecmwf.exporter import ODBExporter, ODBExporterConfig, VarNoMapping
from eumetnet.netatmo_metadata import netatmo_metadata
from eumetnet.netatmo_processing import process_netatmo_geojson_messages_to_df
from eumetnet.netatmo_qc_metadata import netatmo_qc_metadata
from ionbeam_client.arrow_tools import _enforce_schema, schema_from_ingestion_map
from ionbeam_client.models import CanonicalStandard, IngestionMetadata

logger = structlog.get_logger(__name__)

_S = CanonicalStandard

NETATMO_VARIABLE_MAP = [
    VarNoMapping(varno=39, mapped_from=[
        _S(standard_name="air_temperature", level=2.0, method="point", period="PT0S"),
        _S(standard_name="air_temperature", level=2.0, method="point", period="PT10M"),
    ]),
    VarNoMapping(varno=40, mapped_from=[
        _S(standard_name="dew_point_temperature", level=2.0, method="point", period="PT0S"),
    ]),
    VarNoMapping(varno=58, mapped_from=[
        _S(standard_name="relative_humidity", level=2.0, method="point", period="PT0S"),
    ]),
    VarNoMapping(varno=62, mapped_from=[
        _S(standard_name="visibility_in_air", level=2.0, method="point", period="PT0S"),
    ]),
    VarNoMapping(varno=107, mapped_from=[
        _S(standard_name="surface_air_pressure", level=2.0, method="point", period="PT0S"),
        _S(standard_name="surface_air_pressure", level=2.0, method="point", period="PT10M"),
    ]),
    VarNoMapping(varno=108, mapped_from=[
        _S(standard_name="air_pressure_at_mean_sea_level", level=2.0, method="mean", period="PT1H"),
    ]),
    VarNoMapping(varno=111, mapped_from=[
        _S(standard_name="wind_from_direction", level=2.0, method="mean", period="PT5M"),
    ]),
    VarNoMapping(varno=112, mapped_from=[
        _S(standard_name="wind_speed", level=2.0, method="mean", period="PT5M"),
    ]),
    VarNoMapping(varno=261, mapped_from=[
        _S(standard_name="wind_speed_of_gust", level=2.0, method="mean", period="PT5M"),
    ]),
]


def _build_column_rename(metadata: IngestionMetadata) -> dict[str, str]:
    """Map DataFrame column names to canonical names the exporter expects."""
    rename = {}
    for var in metadata.ingestion_map.canonical_variables:
        canonical = var.to_canonical_name()
        if var.column != canonical:
            rename[var.column] = canonical
    return rename


def _parse_member_path(name: str) -> tuple[str | None, int | None]:
    """Extract country code and hour from a tar member path.

    Handles both raw and QC layouts:
      20260112/gb/2026011211.jsonl       → ("gb", 11)
      20260112/qc/gb/2026011211.jsonl    → ("gb", 11)
    """
    p = pathlib.PurePosixPath(name)
    parts = p.parts
    if len(parts) < 2:
        return None, None

    # Country is the directory immediately before the filename
    country = parts[-2].lower() if parts[-2] != "qc" else None

    # Hour is the last 2 digits of the stem (YYYYMMDDHH)
    stem = p.stem
    try:
        hour = int(stem[-2:])
    except (ValueError, IndexError):
        hour = None

    return country, hour


def _parse_jsonl_member(tar: tarfile.TarFile, member: tarfile.TarInfo) -> Iterator[dict]:
    """Yield parsed dicts from one JSONL tar member."""
    f = tar.extractfile(member)
    if f is None:
        return
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            continue


def _batched(iterable: Iterator, n: int) -> Iterator[list]:
    """Yield successive n-sized lists from an iterator."""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= n:
            yield batch
            batch = []
    if batch:
        yield batch


def _stream_to_odb(
    messages: Iterator[dict],
    metadata: IngestionMetadata,
    schema: pa.Schema,
    column_rename: dict[str, str],
    exporter: ODBExporter,
    odb_fh,
    batch_size: int,
) -> int:
    """Process a message iterator in chunks, writing ODB frames to an open file handle.

    Returns total ODB rows written.
    """
    # Build the renamed schema the exporter expects (canonical __-separated names)
    odb_schema = pa.schema([
        pa.field(column_rename.get(f.name, f.name), f.type) for f in schema
    ])

    rows_written = 0

    for msg_batch in _batched(messages, batch_size):
        df = process_netatmo_geojson_messages_to_df(msg_batch)
        if df.empty:
            continue

        aligned = _enforce_schema(df, schema)
        renamed = aligned.rename(columns=column_rename)
        table = pa.Table.from_pandas(renamed, schema=odb_schema, preserve_index=False)

        for batch in table.to_batches():
            odb_table = exporter._map_canonical_batch_to_odb(metadata.dataset.name, batch)
            if odb_table is None:
                continue
            odc.encode_odb(odb_table.to_pandas(), odb_fh)
            rows_written += odb_table.num_rows

    return rows_written


@click.command()
@click.argument("archives", nargs=-1, required=True, type=click.Path(exists=True, path_type=pathlib.Path))
@click.option("-o", "--output", required=True, type=click.Path(path_type=pathlib.Path), help="Output directory for ODB files")
@click.option("-c", "--country", "countries", multiple=True, help="Country codes to include (repeatable, e.g. -c gb -c fr). Default: all.")
@click.option("--hour", "hours", multiple=True, type=int, help="Hours to include (repeatable, e.g. --hour 6 --hour 12). Default: all.")
@click.option("--raw-only", is_flag=True, default=False, help="Process only raw data, skip QC files.")
@click.option("--qc-only", is_flag=True, default=False, help="Process only QC data, skip raw files.")
@click.option("--batch-size", default=50_000, type=int, help="Messages per processing batch (controls memory usage)")
def main(
    archives: tuple[pathlib.Path, ...],
    output: pathlib.Path,
    countries: tuple[str, ...],
    hours: tuple[int, ...],
    raw_only: bool,
    qc_only: bool,
    batch_size: int,
):
    """Convert NetAtmo archive tar.gz files to ECMWF ODB format."""
    if raw_only and qc_only:
        raise click.UsageError("--raw-only and --qc-only are mutually exclusive.")

    country_filter = {c.lower() for c in countries}
    hour_filter = set(hours)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )

    exporter = ODBExporter(ODBExporterConfig(
        output_path=output,
        variable_map=NETATMO_VARIABLE_MAP,
    ))

    raw_schema = schema_from_ingestion_map(netatmo_metadata.ingestion_map)
    qc_schema = schema_from_ingestion_map(netatmo_qc_metadata.ingestion_map)
    raw_rename = _build_column_rename(netatmo_metadata)
    qc_rename = _build_column_rename(netatmo_qc_metadata)

    if country_filter or hour_filter or raw_only or qc_only:
        logger.info(
            "Filters active",
            countries=sorted(country_filter) or "all",
            hours=sorted(hour_filter) or "all",
            streams="raw" if raw_only else "qc" if qc_only else "all",
        )

    for archive_path in archives:
        logger.info("Processing archive", path=str(archive_path))
        archive_name = archive_path.stem.removesuffix(".tar")

        output.mkdir(parents=True, exist_ok=True)

        # Build filename suffix from active filters
        suffix_parts = []
        if country_filter:
            suffix_parts.append("_".join(sorted(country_filter)))
        if hour_filter:
            suffix_parts.append("h" + "-".join(str(h) for h in sorted(hour_filter)))
        filter_suffix = "_" + "_".join(suffix_parts) if suffix_parts else ""

        raw_path = output / f"{archive_name}_{netatmo_metadata.dataset.name}{filter_suffix}.odb"
        qc_path = output / f"{archive_name}_{netatmo_qc_metadata.dataset.name}{filter_suffix}.odb"

        raw_rows = 0
        qc_rows = 0
        file_count = 0
        bytes_read = 0
        t0 = time.monotonic()

        with open(raw_path, "wb") as raw_fh, open(qc_path, "wb") as qc_fh:
            with tarfile.open(archive_path, "r:gz") as tar:
                for member in tar:
                    if not member.isfile() or not member.name.endswith(".jsonl"):
                        continue

                    is_qc = "/qc/" in member.name

                    if raw_only and is_qc:
                        continue
                    if qc_only and not is_qc:
                        continue

                    country, hour = _parse_member_path(member.name)
                    if country_filter and (country is None or country not in country_filter):
                        continue
                    if hour_filter and (hour is None or hour not in hour_filter):
                        continue

                    file_count += 1
                    messages = _parse_jsonl_member(tar, member)

                    if is_qc:
                        qc_rows += _stream_to_odb(
                            messages, netatmo_qc_metadata, qc_schema,
                            qc_rename, exporter, qc_fh, batch_size,
                        )
                    else:
                        raw_rows += _stream_to_odb(
                            messages, netatmo_metadata, raw_schema,
                            raw_rename, exporter, raw_fh, batch_size,
                        )

                    bytes_read += member.size
                    elapsed = time.monotonic() - t0
                    logger.info(
                        "Progress",
                        files=file_count,
                        raw_rows=raw_rows,
                        qc_rows=qc_rows,
                        data_read_gb=f"{bytes_read / 1e9:.2f}",
                        elapsed=f"{elapsed:.0f}s",
                    )

        for path, rows, label in [
            (raw_path, raw_rows, "raw"),
            (qc_path, qc_rows, "qc"),
        ]:
            if rows == 0:
                path.unlink(missing_ok=True)
                logger.info("No data for stream", stream=label, archive=archive_name)
            else:
                logger.info("ODB written", stream=label, output=str(path), rows=rows)

    logger.info("Done", archives_processed=len(archives))
