from typing import List, Optional

from ..enum.analysis import RasterLayer
from .base import StrictBaseModel
from .geostore import Geometry


class ZonalAnalysisRequestIn(StrictBaseModel):
    geometry: Geometry
    sum: List[RasterLayer]
    group_by: List[RasterLayer] = list()
    filters: List[RasterLayer] = list()
    start_date: Optional[str] = None
    end_date: Optional[str] = None
