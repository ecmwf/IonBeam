# syntax=docker/dockerfile:1.7-labs

# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

# Every dependency in uv.lock installs as a prebuilt wheel on glibc (pyogrio
# vendors GDAL, cf-units vendors udunits2), so no stage needs a compiler.
# cf-units wheels are x86_64-only: build the ecmwf-exporter target with
# --platform linux/amd64 on other hosts.

FROM python:3.12-slim AS runtime-base

RUN useradd --uid 1000 --user-group --create-home ionbeam
USER ionbeam
WORKDIR /app
ENV PATH="/venv/bin:${PATH}"

FROM python:3.12-slim AS builder-base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/venv

WORKDIR /workspace

# Manifests only, so the dependency layer caches until the lockfile changes.
COPY --parents pyproject.toml uv.lock **/pyproject.toml /workspace/

# Full source with the CI-determined version stamped into every workspace
# member, so installed package metadata (and e.g. ODB creaby@desc) carries the
# release version. Stamping here, after the manifest-only layer above, keeps
# the dependency cache immune to version churn. Local builds without
# --build-arg VERSION keep the placeholder pyproject versions.
FROM builder-base AS sources

COPY . /workspace
ARG VERSION
RUN if [ -n "$VERSION" ]; then \
        find /workspace -maxdepth 3 -name pyproject.toml \
            -exec sed -i "s/^version = .*/version = \"${VERSION}\"/" {} +; \
    fi

# ============================================================================
# IONBEAM - Main service
# ============================================================================
FROM builder-base AS ionbeam-build

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --no-install-workspace --package ionbeam
COPY --from=sources /workspace /workspace
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --package ionbeam

FROM runtime-base AS ionbeam

COPY --from=ionbeam-build /venv /venv
EXPOSE 8815
CMD ["ionbeam", "start"]

# ============================================================================
# DATA SOURCES - union image; the pod picks its component via its command
# ============================================================================
FROM builder-base AS data-sources-build

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-editable --no-install-workspace --only-group data-sources
COPY --from=sources /workspace /workspace
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-editable --only-group data-sources

FROM runtime-base AS data-sources

COPY --from=data-sources-build /venv /venv

# ============================================================================
# ECMWF EXPORTER
# ============================================================================
FROM builder-base AS ecmwf-exporter-build

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --no-install-workspace --package ecmwf
COPY --from=sources /workspace /workspace
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --package ecmwf

FROM runtime-base AS ecmwf-exporter

# cf-units' vendored libudunits2 links libexpat, which slim omits.
USER root
RUN apt-get update && apt-get install -y --no-install-recommends libexpat1 \
    && rm -rf /var/lib/apt/lists/*
USER ionbeam

COPY --from=ecmwf-exporter-build /venv /venv
CMD ["ecmwf-exporter"]

# ============================================================================
# LEGACY API
# ============================================================================
FROM builder-base AS legacy-api-build

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --no-install-workspace --package ionbeam-legacy-api
COPY --from=sources /workspace /workspace
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --package ionbeam-legacy-api

FROM runtime-base AS legacy-api

COPY --from=legacy-api-build /venv /venv
EXPOSE 8080
CMD ["ionbeam-legacy-api"]

