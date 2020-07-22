from enum import Enum


class SourceType(str, Enum):
    raster = "raster"
    table = "table"
    vector = "vector"


class RasterSourceType(str, Enum):
    raster = "raster"


class TableSourceType(str, Enum):
    __doc__ = "Source type of input file."
    table = "table"


class VectorSourceType(str, Enum):
    vector = "vector"
