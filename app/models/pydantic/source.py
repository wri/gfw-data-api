from enum import Enum
from typing import Optional, List

from pydantic import BaseModel


class SourceType(str, Enum):
    raster = "raster"
    table = "table"
    vector = "vector"


class Source(BaseModel):
    source_uri: Optional[List[str]]
