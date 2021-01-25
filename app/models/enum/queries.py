from enum import Enum


class QueryFormat(str, Enum):
    json = "json"
    csv = "csv"
