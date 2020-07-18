from typing import List

from aenum import Enum, extend_enum


class PGOtherType(str, Enum):
    __doc__ = "Other PostgreSQL data types"
    array = "ARRAY"
    boolean = "boolean"
    jsonb = "jsonb"
    time = "time"
    uuid = "uuid"
    xml = "xml"


class PGNumericType(str, Enum):
    __doc__ = "Numeric PostgreSQL data types"
    bigint = "bigint"
    double_precision = "double precision"
    integer = "integer"
    numeric = "numeric"
    smallint = "smallint"


class PGTextType(str, Enum):
    __doc__ = "Text PostgreSQL data types"
    character_varying = "character varying"
    text = "text"


class PGDateType(str, Enum):
    __doc__ = "Date PostgreSQL data types"
    date = "date"
    timestamp = "timestamp"
    timestamp_wtz = "timestamp without time zone"


class PGGeometryType(str, Enum):
    __doc__ = "Geometry PostgreSQL data types"
    geometry = "geometry"


class PGType(str, Enum):
    __doc__ = "PostgreSQL data type enumeration"


# extent PGTYPE with types listed above
sub_classes: List[Enum] = [
    PGDateType,
    PGTextType,
    PGNumericType,
    PGGeometryType,
    PGOtherType,
]
for sub_class in sub_classes:
    for field in sub_class:
        extend_enum(PGType, field.name, field.value)
