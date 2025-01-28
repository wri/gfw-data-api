from typing import Optional, List

from app.models.enum.creation_options import Delimiters
from app.models.pydantic.base import StrictBaseModel
from app.models.pydantic.geostore import FeatureCollection, Geometry
from pydantic import Field


class QueryRequestIn(StrictBaseModel):
    geometry: Optional[Geometry]
    sql: str


class QueryBatchRequestIn(StrictBaseModel):
    feature_collection: Optional[FeatureCollection] = Field(
        None, description="An inline collection of GeoJson features on which to do the same query"
    )
    uri: Optional[str] = Field(
        None, description="URI to a vector file in a variety of formats supported by Geopandas, including GeoJson and CSV format, giving a list of features on which to do the same query. For a CSV file, the column with the geometry in WKB format should be named 'WKT' (not 'WKB')"
    )
    geostore_ids: Optional[List[str]] = Field(
        None, description="An inline list of ResourceWatch geostore ids"
    )
    id_field: str = Field(
        "fid", description="Name of field with the feature id, for use in labeling the results for each feature. This field must contain a unique value for each feature. If geostore_ids are specified, then they will automatically be used as the feature id for labeling the results."
    )
    sql: str


class CsvQueryRequestIn(QueryRequestIn):
    delimiter: Delimiters = Delimiters.comma
