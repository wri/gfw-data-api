from datetime import datetime

from pydantic import BaseModel


class BaseRecord(BaseModel):
    created_on: datetime
    updated_on: datetime

    class Config:
        orm_mode = True
