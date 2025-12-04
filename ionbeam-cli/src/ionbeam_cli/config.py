# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from pydantic import BaseModel, Field


class CLIConfig(BaseModel):
    amqp_url: str = Field(
        default="amqp://guest:guest@localhost:5672/",
        description="AMQP connection URL for RabbitMQ",
    )
    connection_timeout: int = Field(
        default=30,
        description="AMQP connection timeout in seconds",
    )