from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class SourceType(str, Enum):
    raster = "raster"
    table = "table"
    vector = "vector"


class Source(BaseModel):
    source_uri: Optional[List[str]]
