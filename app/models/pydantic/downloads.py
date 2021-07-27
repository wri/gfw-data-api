from typing import Optional

from pydantic import Field

from app.models.enum.creation_options import Delimiters
from app.models.pydantic.base import StrictBaseModel
from app.models.pydantic.geostore import Geometry


class DownloadJSONIn(StrictBaseModel):
    sql: str = Field(..., description="SQL query.")
    geometry: Optional[Geometry] = Field(
        None, description="A geojson geometry to be used as spatial filter."
    )
    filename: str = Field("export.json", description="Name of export file.")


class DownloadCSVIn(DownloadJSONIn):
    filename: str = Field("export.csv", description="Name of export file.")
    delimiter: Delimiters = Field(
        Delimiters.comma, description="Delimiter to use for CSV file."
    )
