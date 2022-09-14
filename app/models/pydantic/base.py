from datetime import datetime

from pydantic import BaseModel, Extra


class BaseORMRecord(BaseModel):
    class Config:
        orm_mode = True


class BaseRecord(BaseModel):
    created_on: datetime
    updated_on: datetime

    class Config:
        orm_mode = True


class StrictBaseModel(BaseModel):
    class Config:
        extra = Extra.forbid
        validate_assignment = True

