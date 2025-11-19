from pathlib import Path

import click
import structlog
import uvicorn
import yaml

from .data_service import LegacyAPIDataService
from .routes import create_legacy_router

logger = structlog.get_logger(__name__)


def load_config(config_path: str) -> dict:
    if not Path(config_path).exists():
        return {}
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


@click.command()
@click.option("--config", "-c", default="config.yaml", help="Path to config file")
@click.option("--host", default="0.0.0.0", help="Host to bind to")
@click.option("--port", default=8080, help="Port to bind to")
@click.option("--root-path", default="/legacy", help="Root path for the API")
def main(config: str, host: str, port: int, root_path: str):
    """Legacy API - Provides backwards-compatible API for IonBeam data access."""
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware

    # Load configuration if it exists
    config_dict = load_config(config)

    # Get configuration values with defaults
    legacy_config = config_dict.get("legacy_api", {})
    projection_base_path = Path(
        legacy_config.get("projection_base_path", "./projections/ionbeam-legacy")
    )
    metadata_subdir = legacy_config.get("metadata_subdir", "metadata")
    data_subdir = legacy_config.get("data_subdir", "data")
    max_time_range_days = legacy_config.get("max_time_range_days", 31)
    title = legacy_config.get("title", "Ionbeam Legacy API")
    version = legacy_config.get("version", "0.1.0")

    logger.info(
        "Starting Legacy API",
        projection_base_path=str(projection_base_path),
        host=host,
        port=port,
        root_path=root_path,
    )

    # Create data service
    duckdb_service = LegacyAPIDataService(
        projection_base_path=projection_base_path,
        metadata_subdir=metadata_subdir,
        data_subdir=data_subdir,
    )

    # Create FastAPI application
    app = FastAPI(
        title=title,
        version=version,
        root_path=root_path,
        description="IonBeam API",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(create_legacy_router(duckdb_service, max_time_range_days))

    logger.info("Legacy API starting", host=host, port=port)

    # Run the server
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
