from typing import Optional
from uuid import UUID

from .base import BaseRecord
from .responses import Response


class UserJob(BaseRecord):
    job_id: UUID
    status: str
    download_link: Optional[str]
    progress: Optional[str]


class UserJobResponse(Response):
    data: UserJob
