from typing import List, Optional, Dict, Any

from pydantic import BaseModel, Field

from .geostore import Geometry
from ..enum.analysis import RasterLayer


class ZonalAnalysisRequestIn(BaseModel):
    geometry: Dict[str, Any]
    sum: List[RasterLayer]
    group_by: Optional[List[RasterLayer]] = []
    filters: Optional[List[RasterLayer]] = []
    start_date: Optional[str] = None
    end_date: Optional[str] = None
