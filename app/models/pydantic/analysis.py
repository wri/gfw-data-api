from typing import Any, Dict, List, Optional

from ..enum.analysis import RasterLayer
from .base import DataApiBaseModel


class ZonalAnalysisRequestIn(DataApiBaseModel):
    geometry: Dict[str, Any]
    sum: List[RasterLayer]
    group_by: List[RasterLayer] = list()
    filters: List[RasterLayer] = list()
    start_date: Optional[str] = None
    end_date: Optional[str] = None
