from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class ChangeLog(BaseModel):
    date_time: datetime
    status: str
    message: str
    detail: Optional[str] = None
