from pydantic import BaseModel
from datetime import datetime


class Base(BaseModel):
    created_on: datetime
    updated_on: datetime

    class Config:
        orm_mode = True
        fields = {"type": "$type"}
        allow_population_by_field_name = False
