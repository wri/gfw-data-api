from typing import Optional

from app.models.enum.creation_options import Delimiters
from app.models.pydantic.base import StrictBaseModel
from app.models.pydantic.geostore import FeatureCollection, Geometry


class QueryRequestIn(StrictBaseModel):
    geometry: Optional[Geometry]
    sql: str


class QueryBatchRequestIn(StrictBaseModel):
    feature_collection: Optional[FeatureCollection]
    uri: Optional[str]
    id_field: str
    sql: str


class CsvQueryRequestIn(QueryRequestIn):
    delimiter: Delimiters = Delimiters.comma
