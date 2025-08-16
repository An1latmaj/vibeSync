from pydantic import BaseModel, Field
from typing import Optional
from uuid import UUID

class IngestResponse(BaseModel):
    job_id: UUID
    status: str

class JobRow(BaseModel):
    id: UUID
    user_id: UUID
    source_id: UUID
    status: str
    uploaded_at: Optional[str]
    stats: Optional[dict]
