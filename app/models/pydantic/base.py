from datetime import datetime

from pydantic import BaseModel, Extra


class Base(BaseModel):
    created_on: datetime
    updated_on: datetime

    class Config:
        orm_mode = True


class DataApiBaseModel(BaseModel):
    class Config:
        extra = Extra.forbid
