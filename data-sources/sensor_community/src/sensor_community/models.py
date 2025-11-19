from pydantic import BaseModel


class SensorCommunityConfig(BaseModel):
    base_url: str = "https://archive.sensor.community"
    timeout_seconds: int = 60
    concurrency: int = 10
