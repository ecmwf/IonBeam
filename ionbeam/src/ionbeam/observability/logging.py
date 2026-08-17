# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import logging
import sys

import structlog
from structlog.contextvars import merge_contextvars
from structlog.processors import TimeStamper
from structlog.stdlib import LoggerFactory, ProcessorFormatter


def setup_logging(level: int = logging.INFO) -> None:
    """Route all logging (structlog and stdlib) to stderr: pretty-printed on a
    terminal, JSON lines otherwise. Rotation and shipping belong to the
    platform reading the stream."""
    timestamper = TimeStamper(fmt="iso", utc=True)
    pre_chain = [
        merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.ExtraAdder(),
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    renderer = (
        structlog.dev.ConsoleRenderer(colors=True)
        if sys.stderr.isatty()
        else structlog.processors.JSONRenderer()
    )
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(
        ProcessorFormatter(
            processors=[ProcessorFormatter.remove_processors_meta, renderer],
            foreign_pre_chain=pre_chain,
        )
    )

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    root.addHandler(handler)

    structlog.configure(
        processors=[
            merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.PositionalArgumentsFormatter(),
            timestamper,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
