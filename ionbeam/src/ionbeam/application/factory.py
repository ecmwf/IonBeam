# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Composition root: container -> IonbeamCore behind the Flight endpoint."""

import logging
import signal
import threading
from pathlib import Path

import structlog
from prometheus_client import start_http_server

from ionbeam.container import IonbeamCoreContainer
from ionbeam.flight.server import IonbeamFlightServer
from ionbeam.observability.logging import setup_logging
from ionbeam.scheduler import SourceScheduler

logger = structlog.get_logger(__name__)


def run(host: str, port: int) -> None:
    container = IonbeamCoreContainer()

    log_config = container.config.logging()
    log_level = getattr(logging, log_config.get("level", "INFO"))
    log_dir = Path(log_config.get("log_dir", "./logs"))
    log_name = log_config.get("log_name", "ionbeam.log")
    setup_logging(level=log_level, log_dir=log_dir, log_name=log_name)

    core = container.ionbeam_core()
    registry = container.registry()
    container.dataset_registry().validate_retention(container.record_retention())

    metrics_config = container.config.metrics() or {}
    start_http_server(metrics_config.get("port", 8000), registry=registry)

    scheduler = SourceScheduler(
        container.source_schedules(),
        core.trigger_source,
        container.trigger_claims().try_claim,
    )

    server = IonbeamFlightServer(f"grpc://{host}:{port}", core)
    server.spawn(core.start())
    server.spawn(scheduler.start())

    stop_requested = threading.Event()

    def _request_stop(signum, _frame):
        logger.info("ionbeam received signal; shutting down", signal=signum)
        stop_requested.set()

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    # serve() blocks its thread inside gRPC where Python signal handlers never run,
    # so serve on a worker thread and keep the main thread free to react to signals.
    serve_thread = threading.Thread(target=server.serve, name="flight-serve")
    serve_thread.start()

    logger.info("ionbeam Flight endpoint listening", host=host, port=port)
    stop_requested.wait()

    # Stop the scheduler and builder on the server loop while it still runs,
    # then drain gRPC and stop the loop.
    server.spawn(scheduler.stop()).result()
    server.spawn(core.stop()).result()
    server.shutdown()
    serve_thread.join()
    logger.info("ionbeam stopped")
