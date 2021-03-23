from enum import Enum


class QueryFormat(str, Enum):
    json = "json"
    csv = "csv"

class CsvDelimiter(str, Enum):
    comma = ","
    tab = "\t"