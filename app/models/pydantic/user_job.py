from typing import Optional
from uuid import UUID

from pydantic import BaseModel

from .responses import Response


class UserJob(BaseModel):
    job_id: UUID
    job_link: str = ""
    status: str = "pending"
    download_link: Optional[str] = None
    failed_geometries_link: Optional[str] = None
    progress: Optional[str] = "0%"


class UserJobResponse(Response):
    data: UserJob
