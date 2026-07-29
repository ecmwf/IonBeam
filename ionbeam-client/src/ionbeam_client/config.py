# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from pydantic import BaseModel, Field


class IonbeamClientConfig(BaseModel):
    """Configuration for the Ionbeam client.

    Attributes:
        flight_url: Ionbeam Arrow Flight endpoint. Default: "grpc://localhost:8815"
        retry_delay: Delay before reconnecting a dropped subscription stream, in seconds. Default: 1.0
        shutdown_timeout: Seconds to let an in-flight trigger/export handler finish
            on close before abandoning it. Size to the pod's termination grace
            period so a handler is never cut short mid-work. Default: 25.0
    """

    flight_url: str = Field(default="grpc://localhost:8815")
    retry_delay: float = Field(default=1.0)
    shutdown_timeout: float = Field(default=25.0)
