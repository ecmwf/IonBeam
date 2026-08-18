# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Server-side, per-dataset production config: aggregation span, presentation
metadata, and the rebuild debounce, decided by the server.

Keyed by dataset name and resolved through a :class:`DatasetRegistry`. Ingestion
carries only what a source knows; the server decides how the output dataset is
built, revised, and finalised."""

from datetime import timedelta
from typing import Optional

import structlog
from ionbeam_client.models import DatasetMetadata, Link
from pydantic import BaseModel, field_validator


class DatasetBuildConfig(BaseModel):
    """How the server produces one output dataset. See :class:`DatasetRegistry`."""

    # presentation / windowing: travels outward in built-dataset schema metadata
    description: str = ""
    aggregation_span: timedelta = timedelta(days=1)
    source_links: list[Link] = []
    keywords: list[str] = []

    @field_validator("aggregation_span")
    @classmethod
    def _nests_within_a_day(cls, span: timedelta) -> timedelta:
        seconds = span.total_seconds()
        if seconds <= 0 or seconds != int(seconds) or 86400 % int(seconds):
            raise ValueError(
                "aggregation_span must be a whole number of seconds dividing "
                "one day: windows are epoch-aligned, so a day-dividing span "
                "nests every window inside the ib_day partition its build is "
                "stored under"
            )
        return span

    # coordination only; never leaves the server
    rebuild_debounce: timedelta = timedelta(0)
    # suppresses redelivered row content at ingestion, against exact per-window
    # fingerprint sets, for sources that re-fetch history
    dedup_ingestion: bool = False

    def output_metadata(self, name: str) -> DatasetMetadata:
        """The presentation fields this config shares with
        :class:`DatasetMetadata`, stamped with ``name``."""
        shared = self.model_dump(include=set(DatasetMetadata.model_fields) - {"name"})
        return DatasetMetadata(name=name, **shared)

    def seal_delay(self, retention: timedelta) -> timedelta:
        """From window start to seal — the span plus the hot-store retention.
        Past it, arrivals can no longer affect any build of the window."""
        return self.aggregation_span + retention


class DatasetRegistry:
    """Resolves a dataset name to its build config, falling back to a default
    so an unconfigured dataset still ingests and builds."""

    def __init__(
        self,
        configs: Optional[dict[str, DatasetBuildConfig]] = None,
        default: Optional[DatasetBuildConfig] = None,
    ) -> None:
        self._configs = configs or {}
        self._default = default or DatasetBuildConfig()
        self._logger = structlog.get_logger(__name__)
        self._warned_unconfigured: set[str] = set()

    def configured(self, name: str) -> bool:
        return name in self._configs

    def get(self, name: str) -> DatasetBuildConfig:
        config = self._configs.get(name)
        if config is not None:
            return config
        if name not in self._warned_unconfigured:
            self._warned_unconfigured.add(name)
            self._logger.warning(
                "No build config for dataset; building on defaults",
                dataset=name,
                configured=sorted(self._configs),
            )
        return self._default

    @classmethod
    def from_config(cls, cfg: Optional[dict]) -> "DatasetRegistry":
        cfg = cfg or {}
        defaults = cfg.get("defaults") or {}
        default = DatasetBuildConfig(**defaults)
        # entries inherit `defaults` per key: one override never resets the rest
        registry = {
            name: DatasetBuildConfig(**{**defaults, **(entry or {})})
            for name, entry in (cfg.get("registry") or {}).items()
        }
        return cls(registry, default)
