from datetime import datetime
from typing import List, Optional

from ..enum.change_log import ChangeLogStatus, ChangeLogStatusTaskIn
from .base import DataApiBaseModel
from .responses import Response


class ChangeLog(DataApiBaseModel):
    date_time: datetime
    status: ChangeLogStatus
    message: str
    detail: Optional[str] = None


class ChangeLogTaskIn(DataApiBaseModel):
    date_time: datetime
    status: ChangeLogStatusTaskIn
    message: str
    detail: Optional[str] = None


class ChangeLogResponse(Response):
    data: List[ChangeLog]
