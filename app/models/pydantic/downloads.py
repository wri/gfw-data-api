from typing import Optional

from pydantic import Field

from app.models.pydantic.base import StrictBaseModel
from app.models.pydantic.geostore import Geometry


class DownloadCSVIn(StrictBaseModel):
    sql: str = Field(..., description="SQL query.")
    geometry: Optional[Geometry] = Field(
        None, description="A geojson geometry to be used as spatial filter."
    )
    filename: str = Field("export.csv", description="Name of export file.")
    delimiter: str = Field(",", description="Delimiter to use for CSV file.")
