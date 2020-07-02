from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from ..enum.change_log import ChangeLogStatus


class ChangeLog(BaseModel):
    date_time: datetime
    status: ChangeLogStatus
    message: str
    detail: Optional[str] = None
