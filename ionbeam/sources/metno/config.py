from pydantic import BaseModel


class NetAtmoMQTTConfig(BaseModel):
    host: str
    username: str
    password: str
    client_id: str
    port: int = 8883
    keepalive: int = 120
    use_tls: bool = True
    flush_interval_seconds: int = 60
    flush_max_records: int = 50000
    max_buffer_size: int = 200000
    url: str
    routing_key: str = "ionbeam.ingestion.ingestV1"

class NetAtmoHTTPConfig(BaseModel):
    base_url: str
    username: str
    password: str
    timeout_seconds: int = 60
    concurrency: int = 8
