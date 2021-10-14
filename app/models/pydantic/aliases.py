from .base import StrictBaseModel
from .responses import Response


class Alias(StrictBaseModel):
    alias: str
    version: str
    dataset: str

    class Config:
        orm_mode = True


class AliasResponse(Response):
    data: Alias


class AliasCreateIn(StrictBaseModel):
    version: str
