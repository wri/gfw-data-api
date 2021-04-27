from enum import Enum


class QueryFormat(str, Enum):
    json = "json"
    csv = "csv"


class QueryType(str, Enum):
    table = "table"
    raster = "raster"
