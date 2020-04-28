from enum import Enum
from typing import Optional, List

from pydantic import BaseModel


class SourceType(str, Enum):
    vector = "vector"
    raster = "raster"


class Source(BaseModel):
    source_uri: Optional[List[str]]
