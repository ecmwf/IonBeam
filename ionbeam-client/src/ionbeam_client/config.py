# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from pydantic import BaseModel, Field


class IonbeamClientConfig(BaseModel):
    amqp_url: str = Field(default="amqp://guest:guest@localhost:5672/")
    connection_timeout: int = Field(default=30)
    max_retries: int = Field(default=3)
    retry_delay: float = Field(default=1.0)
    write_batch_size: int = Field(default=65536)


_INGESTION_EXCHANGE: str = "ionbeam.ingestion"
_INGESTION_ROUTING_KEY: str = "ingestV1"

# Arrow store configuration - will migrate to S3-compatible object store
_ARROW_STORE_PATH: str = "/data"
