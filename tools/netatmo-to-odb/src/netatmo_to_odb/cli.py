import asyncio
import json
import pathlib
import tarfile

import click
import pyarrow as pa
import pyodc as odc
import structlog

from ecmwf.exporter import ODBExporter, ODBExporterConfig, VarNoMapping
from eumetnet.netatmo_metadata import netatmo_metadata
from eumetnet.netatmo_processing import process_netatmo_geojson_messages_to_df
from eumetnet.netatmo_qc_metadata import netatmo_qc_metadata
from ionbeam_client.arrow_tools import dataframes_to_record_batches, schema_from_ingestion_map
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


def read_archive(path: pathlib.Path) -> tuple[list[dict], list[dict]]:
    """Read a netatmo archive tar.gz and separate raw vs QC messages."""
    raw_messages: list[dict] = []
    qc_messages: list[dict] = []

    with tarfile.open(path, "r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile() or not member.name.endswith(".jsonl"):
                continue

            f = tar.extractfile(member)
            if f is None:
                continue

            is_qc = "/qc/" in member.name
            target = qc_messages if is_qc else raw_messages

            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    target.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    return raw_messages, qc_messages


def process_dataset(
    messages: list[dict],
    metadata: IngestionMetadata,
    exporter: ODBExporter,
    output_dir: pathlib.Path,
    archive_name: str,
) -> pathlib.Path | None:
    """Process a set of messages through the full pipeline to ODB."""
    if not messages:
        logger.info("No messages for dataset", dataset=metadata.dataset.name)
        return None

    logger.info(
        "Processing dataset",
        dataset=metadata.dataset.name,
        message_count=len(messages),
    )

    df = process_netatmo_geojson_messages_to_df(messages)
    if df.empty:
        logger.warning("Empty DataFrame after processing", dataset=metadata.dataset.name)
        return None

    logger.info("DataFrame ready", dataset=metadata.dataset.name, rows=len(df), columns=list(df.columns))

    schema = schema_from_ingestion_map(metadata.ingestion_map)

    async def _to_batches():
        async for batch in dataframes_to_record_batches([df], schema=schema):
            yield batch

    odb_tables: list[pa.Table] = []
    for batch in asyncio.run(_collect_batches(_to_batches())):
        odb_table = exporter._map_canonical_batch_to_odb(metadata.dataset.name, batch)
        if odb_table is not None:
            odb_tables.append(odb_table)

    if not odb_tables:
        logger.warning("No ODB data generated", dataset=metadata.dataset.name)
        return None

    combined = pa.concat_tables(odb_tables).to_pandas()

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{archive_name}_{metadata.dataset.name}.odb"
    odc.encode_odb(combined, str(output_file))

    logger.info(
        "ODB written",
        dataset=metadata.dataset.name,
        output=str(output_file),
        rows=len(combined),
    )
    return output_file


async def _collect_batches(batch_stream) -> list[pa.RecordBatch]:
    batches = []
    async for batch in batch_stream:
        batches.append(batch)
    return batches


@click.command()
@click.argument("archives", nargs=-1, required=True, type=click.Path(exists=True, path_type=pathlib.Path))
@click.option("-o", "--output", required=True, type=click.Path(path_type=pathlib.Path), help="Output directory for ODB files")
def main(archives: tuple[pathlib.Path, ...], output: pathlib.Path):
    """Convert NetAtmo archive tar.gz files to ECMWF ODB format."""
    structlog.configure(
        processors=[
            structlog.dev.ConsoleRenderer(),
        ],
    )

    exporter = ODBExporter(ODBExporterConfig(
        output_path=output,
        variable_map=NETATMO_VARIABLE_MAP,
    ))

    for archive_path in archives:
        logger.info("Processing archive", path=str(archive_path))
        archive_name = archive_path.stem.removesuffix(".tar")

        raw_messages, qc_messages = read_archive(archive_path)
        logger.info(
            "Archive read",
            raw_count=len(raw_messages),
            qc_count=len(qc_messages),
        )

        process_dataset(raw_messages, netatmo_metadata, exporter, output, archive_name)
        process_dataset(qc_messages, netatmo_qc_metadata, exporter, output, archive_name)

    logger.info("Done", archives_processed=len(archives))
