# syntax=docker/dockerfile:1.7-labs

# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

# Multi-target build. Targets: ionbeam, data-sources, ecmwf-exporter, legacy-api
#
#   docker build --target ionbeam .
#
# The wheels install into the same python:3.12-slim that runs them, because the
# `dist` stage below builds them here rather than on a CI runner. CI extracts
# that stage for the PyPI publish, so the uploaded wheels are the ones shipped
# in the images:
#
#   docker build --target dist --build-arg VERSION=1.2.3 --output type=local,dest=dist .
#
# Every dependency in uv.lock installs as a prebuilt wheel on glibc (pyogrio
# vendors GDAL, cf-units vendors udunits2), so no stage needs a compiler.
# cf-units wheels are x86_64-only: build the ecmwf-exporter target with
# --platform linux/amd64 on other hosts.

FROM python:3.12-slim AS runtime-base

RUN useradd --uid 1000 --user-group --create-home ionbeam
USER ionbeam
WORKDIR /app
ENV PATH="/venv/bin:${PATH}"

FROM python:3.12-slim AS uv-base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

WORKDIR /workspace

# ============================================================================
# DIST - workspace wheels and the locked requirements each image installs
# ============================================================================
FROM uv-base AS dist-build

# Manifests first: the export layer caches until the lockfile changes.
COPY --parents pyproject.toml uv.lock **/pyproject.toml /workspace/

# --no-emit-workspace: the wheels supply the members themselves, so these files
# pin only third-party dependencies, with hashes, per image.
RUN mkdir -p /dist/requirements && \
    for pkg in ionbeam ionbeam-legacy-api ecmwf; do \
        uv export --frozen --no-emit-workspace --no-dev --package "$pkg" \
            --format requirements-txt -o "/dist/requirements/${pkg}.txt"; \
    done && \
    uv export --frozen --no-emit-workspace --no-dev --only-group data-sources \
        --format requirements-txt -o /dist/requirements/data-sources.txt

COPY . /workspace

# Stamped after the export layer so version churn leaves the pins cached. A
# build without VERSION keeps the placeholder pyproject versions.
ARG VERSION
RUN if [ -n "$VERSION" ]; then \
        find /workspace -maxdepth 3 -name pyproject.toml \
            -exec sed -i "s/^version = .*/version = \"${VERSION}\"/" {} +; \
    fi && \
    uv build --all-packages --wheel --out-dir /dist

# Wheels and requirements alone, for `--output type=local` to the host.
FROM scratch AS dist

COPY --from=dist-build /dist/ /

FROM uv-base AS installer-base

COPY --from=dist-build /dist/requirements/ /requirements/

FROM installer-base AS ionbeam-install

RUN uv venv /venv && uv pip install --python /venv --no-cache -r /requirements/ionbeam.txt
COPY --from=dist-build /dist/*.whl /wheels/
RUN uv pip install --python /venv --no-cache --no-deps \
        /wheels/ionbeam-*.whl /wheels/ionbeam_client-*.whl

FROM runtime-base AS ionbeam

COPY --from=ionbeam-install /venv /venv
EXPOSE 8815
CMD ["ionbeam", "start"]

# ============================================================================
# DATA SOURCES - union image; the pod picks its component via its command
# ============================================================================
FROM installer-base AS data-sources-install

RUN uv venv /venv && uv pip install --python /venv --no-cache -r /requirements/data-sources.txt
COPY --from=dist-build /dist/*.whl /wheels/
RUN uv pip install --python /venv --no-cache --no-deps \
        /wheels/acronet-*.whl \
        /wheels/eumetnet-*.whl \
        /wheels/ioncannon-*.whl \
        /wheels/meteotracker-*.whl \
        /wheels/sensor_community-*.whl \
        /wheels/ionbeam_client-*.whl

FROM runtime-base AS data-sources

COPY --from=data-sources-install /venv /venv

# ============================================================================
# ECMWF EXPORTER
# ============================================================================
FROM installer-base AS ecmwf-exporter-install

RUN uv venv /venv && uv pip install --python /venv --no-cache -r /requirements/ecmwf.txt
COPY --from=dist-build /dist/*.whl /wheels/
RUN uv pip install --python /venv --no-cache --no-deps \
        /wheels/ecmwf-*.whl /wheels/ionbeam_client-*.whl

FROM runtime-base AS ecmwf-exporter

# cf-units' vendored libudunits2 links libexpat, which slim omits.
USER root
RUN apt-get update && apt-get install -y --no-install-recommends libexpat1 \
    && rm -rf /var/lib/apt/lists/*
USER ionbeam

COPY --from=ecmwf-exporter-install /venv /venv
CMD ["ecmwf-exporter"]

# ============================================================================
# LEGACY API
# ============================================================================
FROM installer-base AS legacy-api-install

RUN uv venv /venv && uv pip install --python /venv --no-cache -r /requirements/ionbeam-legacy-api.txt
COPY --from=dist-build /dist/*.whl /wheels/
RUN uv pip install --python /venv --no-cache --no-deps \
        /wheels/ionbeam_legacy_api-*.whl /wheels/ionbeam-*.whl /wheels/ionbeam_client-*.whl

FROM runtime-base AS legacy-api

COPY --from=legacy-api-install /venv /venv
EXPOSE 8080
CMD ["ionbeam-legacy-api"]
