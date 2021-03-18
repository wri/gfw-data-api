from typing import Optional

from app.models.pydantic.base import StrictBaseModel
from app.models.pydantic.geostore import Geometry


class QueryRequestIn(StrictBaseModel):
    geometry: Optional[Geometry]
    sql: str
