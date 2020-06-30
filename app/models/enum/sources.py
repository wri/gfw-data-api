from enum import Enum


class SourceType(str, Enum):
    raster = "raster"
    table = "table"
    vector = "vector"
