from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel

from ..enum.change_log import ChangeLogStatus
from .responses import Response


class ChangeLog(BaseModel):
    date_time: datetime
    status: ChangeLogStatus
    message: str
    detail: Optional[str] = None


class ChangeLogResponse(Response):
    data: List[ChangeLog]
