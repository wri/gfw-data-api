from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class ChangeLogStatus(str, Enum):
    success = "success"
    pending = "pending"
    failed = "failed"


class ChangeLog(BaseModel):
    date_time: datetime
    status: ChangeLogStatus
    message: str
    detail: Optional[str] = None
