from datetime import datetime
from typing import List, Optional

from ..enum.change_log import ChangeLogStatus, ChangeLogStatusTaskIn
from .base import StrictBaseModel
from .responses import Response


class ChangeLog(StrictBaseModel):
    date_time: datetime
    status: ChangeLogStatus
    message: str
    detail: Optional[str] = None


class ChangeLogTaskIn(StrictBaseModel):
    date_time: datetime
    status: ChangeLogStatusTaskIn
    message: str
    detail: Optional[str] = None


class ChangeLogResponse(Response):
    data: List[ChangeLog]
