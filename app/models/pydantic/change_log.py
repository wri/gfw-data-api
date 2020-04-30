from datetime import datetime
from pydantic import BaseModel


class ChangeLog(BaseModel):
    date_time: datetime
    status: str
    message: str
