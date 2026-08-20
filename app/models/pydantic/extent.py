from typing import Optional

from .responses import Response
from .geostore import FeatureCollection


class Extent(FeatureCollection):
    pass


class ExtentResponse(Response):
    data: Optional[Extent]
