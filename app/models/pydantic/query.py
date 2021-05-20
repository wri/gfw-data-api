from typing import Optional

from app.models.enum.creation_options import Delimiters
from app.models.enum.queries import QueryFormat
from app.models.pydantic.base import StrictBaseModel
from app.models.pydantic.geostore import Geometry


class QueryRequestIn(StrictBaseModel):
    geometry: Optional[Geometry]
    sql: str
    format: QueryFormat = QueryFormat.json
    delimter: Delimiters = Delimiters.comma
