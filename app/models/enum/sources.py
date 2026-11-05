from enum import StrEnum


class SourceType(StrEnum):
    raster = "raster"
    table = "table"
    vector = "vector"


class RasterSourceType(StrEnum):
    raster = "raster"


class TableSourceType(StrEnum):
    __doc__ = "Source type of input file."
    table = "table"


class VectorSourceType(StrEnum):
    vector = "vector"
