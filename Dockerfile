# syntax=docker/dockerfile:1.7-labs

FROM python:3.12-alpine AS python-base

# bleeding edge repo for eccodes, udunits
RUN echo "http://dl-cdn.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
    apk add --no-cache build-base python3-dev git gdal-dev udunits udunits-dev eccodes

ENV UDUNITS2_XML_PATH=/usr/share/udunits/udunits2.xml

WORKDIR /workspace

# Stage 1: Base with uv and all workspace code
FROM python-base AS uv-base

# Copy UV from official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

# Copy entire workspace (dockerignore filters out .venv, cache dirs, etc.)
COPY . /workspace

# ============================================================================
# IONBEAM - Main service
# ============================================================================
FROM uv-base AS ionbeam-deps

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/ionbeam \
    uv sync --frozen --no-install-project --no-dev --package ionbeam

FROM ionbeam-deps AS ionbeam-build

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/ionbeam \
    uv sync --frozen --no-dev --no-editable --package ionbeam

FROM python-base AS ionbeam

WORKDIR /app
ENV PATH="/venvs/ionbeam/bin:${PATH}"
COPY --from=ionbeam-build /venvs/ionbeam /venvs/ionbeam
# # USER app
CMD ["ionbeam", "start", "--with-builder"]

# ============================================================================
# DATA SOURCES
# ============================================================================

# ACRONET
FROM uv-base AS acronet-deps

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/acronet \
    uv sync --frozen --no-install-project --no-dev --package acronet

FROM acronet-deps AS acronet-build

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/acronet \
    uv sync --frozen --no-dev --no-editable --package acronet

FROM python-base AS acronet

WORKDIR /app
ENV PATH="/venvs/acronet/bin:${PATH}"
COPY --from=acronet-build /venvs/acronet /venvs/acronet
CMD ["acronet"]

# EUMETNET
FROM uv-base AS eumetnet-deps

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/eumetnet \
    uv sync --frozen --no-install-project --no-dev --package eumetnet

FROM eumetnet-deps AS eumetnet-build

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/eumetnet \
    uv sync --frozen --no-dev --no-editable --package eumetnet

FROM python-base AS eumetnet

WORKDIR /app
ENV PATH="/venvs/eumetnet/bin:${PATH}"
COPY --from=eumetnet-build /venvs/eumetnet /venvs/eumetnet
CMD ["eumetnet"]

# IONCANNON
FROM uv-base AS ioncannon-deps

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/ioncannon \
    uv sync --frozen --no-install-project --no-dev --package ioncannon

FROM ioncannon-deps AS ioncannon-build

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/ioncannon \
    uv sync --frozen --no-dev --no-editable --package ioncannon

FROM python-base AS ioncannon

WORKDIR /app
ENV PATH="/venvs/ioncannon/bin:${PATH}"
COPY --from=ioncannon-build /venvs/ioncannon /venvs/ioncannon
CMD ["ioncannon"]

# METEOGATE
FROM uv-base AS meteogate-deps

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/meteogate \
    uv sync --frozen --no-install-project --no-dev --package meteogate

FROM meteogate-deps AS meteogate-build

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/meteogate \
    uv sync --frozen --no-dev --no-editable --package meteogate

FROM python-base AS meteogate

WORKDIR /app
ENV PATH="/venvs/meteogate/bin:${PATH}"
COPY --from=meteogate-build /venvs/meteogate /venvs/meteogate
CMD ["meteogate"]

# METEOTRACKER
FROM uv-base AS meteotracker-deps

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/meteotracker \
    uv sync --frozen --no-install-project --no-dev --package meteotracker

FROM meteotracker-deps AS meteotracker-build

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/meteotracker \
    uv sync --frozen --no-dev --no-editable --package meteotracker

FROM python-base AS meteotracker

WORKDIR /app
ENV PATH="/venvs/meteotracker/bin:${PATH}"
COPY --from=meteotracker-build /venvs/meteotracker /venvs/meteotracker
CMD ["meteotracker"]

# SENSOR COMMUNITY
FROM uv-base AS sensor-community-deps

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/sensor-community \
    uv sync --frozen --no-install-project --no-dev --package sensor-community

FROM sensor-community-deps AS sensor-community-build

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/sensor-community \
    uv sync --frozen --no-dev --no-editable --package sensor-community

FROM python-base AS sensor-community

WORKDIR /app
ENV PATH="/venvs/sensor-community/bin:${PATH}"
COPY --from=sensor-community-build /venvs/sensor-community /venvs/sensor-community
CMD ["sensor-community"]

# ============================================================================
# EXPORTERS
# ============================================================================

# ECMWF
FROM uv-base AS ecmwf-deps

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/ecmwf \
    uv sync --frozen --no-install-project --no-dev --package ecmwf

FROM ecmwf-deps AS ecmwf-build

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/ecmwf \
    uv sync --frozen --no-dev --no-editable --package ecmwf

FROM python-base AS ecmwf

WORKDIR /app
ENV PATH="/venvs/ecmwf/bin:${PATH}"
COPY --from=ecmwf-build /venvs/ecmwf /venvs/ecmwf
CMD ["ecmwf-exporter"]

# PYGEOAPI
FROM uv-base AS pygeoapi-deps

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/pygeoapi \
    uv sync --frozen --no-install-project --no-dev --package pygeoapi

FROM pygeoapi-deps AS pygeoapi-build

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/pygeoapi \
    uv sync --frozen --no-dev --no-editable --package pygeoapi

FROM python-base AS pygeoapi

WORKDIR /app
ENV PATH="/venvs/pygeoapi/bin:${PATH}"
COPY --from=pygeoapi-build /venvs/pygeoapi /venvs/pygeoapi
CMD ["pygeoapi-exporter"]

# ============================================================================
# LEGACY API
# ============================================================================
FROM uv-base AS legacy-api-deps

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/legacy-api \
    uv sync --frozen --no-install-project --no-dev --package legacy-api

FROM legacy-api-deps AS legacy-api-build

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_PROJECT_ENVIRONMENT=/venvs/legacy-api \
    uv sync --frozen --no-dev --no-editable --package legacy-api

FROM python-base AS legacy-api

WORKDIR /app
ENV PATH="/venvs/legacy-api/bin:${PATH}"
COPY --from=legacy-api-build /venvs/legacy-api /venvs/legacy-api
EXPOSE 8080
CMD ["legacy-api"]
