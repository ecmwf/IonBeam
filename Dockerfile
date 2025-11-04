FROM python:3.12-alpine AS python-base

# bleeding edge repo for eccodes, udunits
RUN echo "http://dl-cdn.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
    apk add --no-cache build-base python3-dev git gdal-dev udunits udunits-dev eccodes

ENV UDUNITS2_XML_PATH=/usr/share/udunits/udunits2.xml

WORKDIR /app

FROM python-base AS builder-proj-deps

# Copy UV from official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_LINK_MODE=copy
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev --no-editable

FROM builder-proj-deps AS builder

COPY . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

FROM python-base AS ionbeam

ENV PATH="/app/.venv/bin:$PATH" \
    VIRTUAL_ENV=/app/.venv

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

CMD ["python3", "-m", "ionbeam", "faststream"]

FROM python-base AS ionbeam-legacy-api

ENV PATH="/app/.venv/bin:$PATH" \
    VIRTUAL_ENV=/app/.venv

WORKDIR /app

# Copy the virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

CMD ["python3", "-m", "ionbeam", "legacy-api"]