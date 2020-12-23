from typing import Optional

from ..pydantic.responses import Response
from .geostore import FeatureCollection


class Extent(FeatureCollection):
    class Config:
        extra = "forbid"


class ExtentResponse(Response):
    data: Optional[Extent]

    class Config:
        extra = "forbid"
