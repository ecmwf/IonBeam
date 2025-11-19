from pydantic import BaseModel


class NetAtmoMQTTConfig(BaseModel):
    host: str
    port: int = 8883
    username: str
    password: str
    client_id: str
    keepalive: int = 120
    use_tls: bool = True
    flush_interval_seconds: int = 60
    flush_max_records: int = 50000
    max_buffer_size: int = 200000
