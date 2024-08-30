from typing import Optional
from uuid import UUID

from pydantic import BaseModel

from .responses import Response


class UserJob(BaseModel):
    job_id: UUID
    job_link: Optional[str]   # Full URL to check the job status
    status: str = "pending"   # Can be pending, success, partial_success, failure, and error
    message: Optional[str]    # Error message when status is "error"
    download_link: Optional[str] = None
    failed_geometries_link: Optional[str] = None
    progress: Optional[str] = "0%"


class UserJobResponse(Response):
    data: UserJob
