from typing import Any, Optional, Union

from pydantic import BaseModel, Field, fields, validator
from sqlalchemy.engine.url import URL
from starlette.datastructures import Secret


class DatabaseURL(BaseModel):
    drivername: str = Field(..., alias="driver", description="The database driver.")
    host: str = Field("localhost", description="Server host.")
    port: Optional[Union[str, int]] = Field(None, description="Server access port.")
    username: Optional[str] = Field(None, alias="user", description="Username")
    password: Optional[Union[str, Secret]] = Field(None, description="Password")
    database: str = Field(..., description="Database name.")
    url: Optional[URL] = None

    class Config:
        arbitrary_types_allowed = True
        allow_population_by_field_name = True

    @validator("url", always=True)
    def build_url(cls, v: Any, field: fields.Field, values: dict):
        if isinstance(v, URL):
            return v
        args = {k: str(v) for k, v in values.items() if v is not None}
        return URL(**args)
