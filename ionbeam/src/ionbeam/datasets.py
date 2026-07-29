# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Server-side, per-dataset production config — the dataset-production concerns a
source has no business owning (aggregation span, presentation metadata, and the
finaliser thresholds that govern revision and immutability).

Keyed by dataset name and resolved through a :class:`DatasetRegistry`, so ingestion
carries only what a source knows and the server decides how the output dataset is
built, revised, and finalised."""

from datetime import timedelta
from typing import Optional

import structlog
from ionbeam_client.models import DatasetMetadata, FeatureType, Link
from pydantic import BaseModel, field_validator


class DatasetProductionConfig(BaseModel):
    """How the server produces one output dataset. See :class:`DatasetRegistry`."""

    # presentation / windowing — travels outward on DataSetAvailableEvent
    description: str = ""
    feature_type: FeatureType = "timeSeries"
    aggregation_span: timedelta = timedelta(days=1)
    source_links: list[Link] = []
    keywords: list[str] = []

    @field_validator("aggregation_span")
    @classmethod
    def _positive(cls, span: timedelta) -> timedelta:
        if span <= timedelta(0):
            raise ValueError("aggregation_span must be positive")
        return span

    # finaliser thresholds (coordination only — never leave the server)
    rebuild_debounce: timedelta = timedelta(0)
    finalize_after: Optional[timedelta] = None  # unset ⇒ hot-store retention
    # expected distinct row-contents per window; sizes the dedup filter, which
    # rescales on overflow rather than erroring
    dedup_capacity: int = 100_000
    # suppress redelivered row content at ingestion (backed by exact
    # fingerprints, never the Bloom filter); for sources that re-fetch history
    dedup_ingestion: bool = False

    def output_metadata(self, name: str) -> DatasetMetadata:
        """The presentation fields this config shares with
        :class:`DatasetMetadata`, stamped with ``name``."""
        shared = self.model_dump(include=set(DatasetMetadata.model_fields) - {"name"})
        return DatasetMetadata(name=name, **shared)

    def finalize_delay(self, retention: timedelta) -> timedelta:
        """The final floor: ``finalize_after``, or the hot-store retention when unset."""
        return self.finalize_after if self.finalize_after is not None else retention

    def seal_delay(self, retention: timedelta) -> timedelta:
        """From window start to seal — the span plus the finalize delay. Past
        it, arrivals can no longer affect any build of the window."""
        return self.aggregation_span + self.finalize_delay(retention)


class DatasetRegistry:
    """Resolves a dataset name to its production config, falling back to a default
    so an unconfigured dataset still ingests and builds."""

    def __init__(
        self,
        configs: Optional[dict[str, DatasetProductionConfig]] = None,
        default: Optional[DatasetProductionConfig] = None,
    ) -> None:
        self._configs = configs or {}
        self._default = default or DatasetProductionConfig()
        self._logger = structlog.get_logger(__name__)
        self._warned_unconfigured: set[str] = set()

    def configured(self, name: str) -> bool:
        return name in self._configs

    def validate_retention(self, retention: timedelta) -> None:
        """Warn when a dataset's finalize floor outlives the record retention —
        its records expire before the seal and late rebuilds can no longer
        compose their record sets."""
        for name, config in self._configs.items():
            if config.finalize_after is not None and config.finalize_after > retention:
                self._logger.warning(
                    "finalize_after exceeds the record retention; "
                    "records will expire before the window seals",
                    dataset=name,
                    finalize_after=str(config.finalize_after),
                    retention=str(retention),
                )

    def get(self, name: str) -> DatasetProductionConfig:
        config = self._configs.get(name)
        if config is not None:
            return config
        # a fallthrough usually means a name typo or a missing entry
        if name not in self._warned_unconfigured:
            self._warned_unconfigured.add(name)
            self._logger.warning(
                "No production config for dataset; building on defaults",
                dataset=name,
                configured=sorted(self._configs),
            )
        return self._default

    @classmethod
    def from_config(cls, cfg: Optional[dict]) -> "DatasetRegistry":
        cfg = cfg or {}
        defaults = cfg.get("defaults") or {}
        default = DatasetProductionConfig(**defaults)
        # entries inherit `defaults` per key, so one override never resets the rest
        registry = {
            name: DatasetProductionConfig(**{**defaults, **(entry or {})})
            for name, entry in (cfg.get("registry") or {}).items()
        }
        return cls(registry, default)
