from enum import Enum


class TableDrivers(str, Enum):
    text = "text"
    # json = "json" # TODO: need to implement this eventually


class VectorDrivers(str, Enum):
    csv = "CSV"
    esrijson = "ESRIJSON"
    file_gdb = "FileGDB"
    geojson = "GeoJSON"
    geojson_seq = "GeoJSONSeq"
    gpkg = "GPKG"
    shp = "ESRI Shapefile"


class Delimiters(str, Enum):
    comma = ","
    tab = "\t"
    pipe = "|"
    semicolon = ";"


class IndexType(str, Enum):
    gist = "gist"
    btree = "btree"
    hash = "hash"


class TileStrategy(str, Enum):
    continuous = "continuous"
    discontinuous = "discontinuous"


class PartitionType(str, Enum):
    hash = "hash"
    list = "list"
    range = "range"


class PGType(str, Enum):
    bigint = "bigint"
    boolean = "boolean"
    character_varying = "character varying"
    date = "date"
    double_precision = "double precision"
    geometry = "geometry"
    integer = "integer"
    jsonb = "jsonb"
    numeric = "numeric"
    smallint = "smallint"
    text = "text"
    time = "time"
    timestamp = "timestamp"
    uuid = "uuid"
    xml = "xml"
