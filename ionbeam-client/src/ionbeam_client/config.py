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
