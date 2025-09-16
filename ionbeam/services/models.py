from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from ..models.models import IngestionMetadata


class IngestionRecord(BaseModel):
    id: UUID
    metadata: IngestionMetadata
    start_time: datetime
    end_time: datetime
