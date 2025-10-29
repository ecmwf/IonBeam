FROM python:3.12-alpine AS python-base

# bleeding edge repo for eccodes, udunits - TODO - build from source?
RUN echo "http://dl-cdn.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories
RUN apk add build-base python3-dev git gdal-dev udunits udunits-dev eccodes
ENV UDUNITS2_XML_PATH=/usr/share/udunits/udunits2.xml

WORKDIR /app

FROM python-base AS ionbeam-builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ADD . /app

RUN ls /app

RUN uv sync --locked
RUN uv build

FROM python-base AS python-install

# COPY dist/*.whl .
COPY --from=ionbeam-builder /app/dist/*.whl .

RUN python3 -m venv /app/.venv && \
    /app/.venv/bin/pip install *.whl

FROM python-install AS ionbeam-faststream

WORKDIR /app

CMD ["/app/.venv/bin/python3","-m", "ionbeam",  "faststream"]

FROM python-install AS ionbeam-legacy-api

WORKDIR /app

CMD ["/app/.venv/bin/python3","-m", "ionbeam",  "legacy-api"]