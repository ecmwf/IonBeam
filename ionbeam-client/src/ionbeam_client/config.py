from pydantic import BaseModel, Field


class IonbeamClientConfig(BaseModel):
    """Configuration for the Ionbeam client.

    This class configures the connection and behavior of the Ionbeam client,
    including AMQP message broker settings and retry behavior.

    Attributes:
        amqp_url: RabbitMQ connection string. Default: "amqp://guest:guest@localhost:5672/"
        connection_timeout: Connection timeout in seconds. Default: 30
        max_retries: Number of retry attempts for failed operations. Default: 3
        retry_delay: Delay between retries in seconds. Default: 1.0
    """
    amqp_url: str = Field(default="amqp://guest:guest@localhost:5672/")
    connection_timeout: int = Field(default=30)
    max_retries: int = Field(default=3)
    retry_delay: float = Field(default=1.0)


_INGESTION_EXCHANGE: str = "ionbeam.ingestion"
_INGESTION_ROUTING_KEY: str = "ingestV1"

# Arrow store configuration - will migrate to S3-compatible object store
_ARROW_STORE_PATH: str = "/data"
