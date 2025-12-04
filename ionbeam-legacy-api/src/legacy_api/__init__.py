import logging
import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .app import main
from .data_service import LegacyAPIDataService
from .routes import create_legacy_router

# Configure logging if not already configured
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

logger = logging.getLogger(__name__)


class LegacyApiConfig(BaseModel):
    title: str = "Ionbeam Legacy API"
    version: str = "0.1.0"
    root_path: str = "/legacy"
    projection_base_path: Path = Path("./projections/ionbeam-legacy")
    metadata_subdir: str = "metadata"
    data_subdir: str = "data"
    max_time_range_days: int = 31


def create_legacy_application(config: LegacyApiConfig | None = None) -> FastAPI:
    app_config = config or LegacyApiConfig()

    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)],
        )

    duckdb_service = LegacyAPIDataService(
        projection_base_path=app_config.projection_base_path,
        metadata_subdir=app_config.metadata_subdir,
        data_subdir=app_config.data_subdir,
    )

    app = FastAPI(
        title=app_config.title,
        version=app_config.version,
        root_path=app_config.root_path,
        description=("IonBeam API"),
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(
        create_legacy_router(duckdb_service, app_config.max_time_range_days)
    )

    return app


__all__ = [
    "main",
    "LegacyApiConfig",
    "create_legacy_application",
    "create_legacy_router",
]
