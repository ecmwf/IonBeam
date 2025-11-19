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