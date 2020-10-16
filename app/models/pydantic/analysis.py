from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from ..enum.analysis import RasterLayer


class ZonalAnalysisRequestIn(BaseModel):
    geometry: Dict[str, Any]
    sum: List[RasterLayer]
    group_by: List[RasterLayer] = list()
    filters: List[RasterLayer] = list()
    start_date: Optional[str] = None
    end_date: Optional[str] = None
