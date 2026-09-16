from enum import StrEnum


class QueryFormat(StrEnum):
    json = "json"
    csv = "csv"


class QueryType(StrEnum):
    table = "table"
    raster = "raster"
