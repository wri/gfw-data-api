from typing import Optional
from uuid import UUID

from pydantic import BaseModel

from .responses import Response


class UserJob(BaseModel):
    job_id: UUID
    status: str
    download_link: Optional[str]
    progress: Optional[str]


class UserJobResponse(Response):
    data: UserJob
