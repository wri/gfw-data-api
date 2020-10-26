from enum import Enum


class RasterDrivers(str, Enum):
    __doc__ = "Raster source driver of input file"
    geotiff = "GeoTIFF"


class TableDrivers(str, Enum):
    __doc__ = "Driver of input file."
    text = "text"
    # json = "json" # TODO: need to implement this eventually


class VectorDrivers(str, Enum):
    __doc__ = "Vector source driver of input file"
    csv = "CSV"
    esrijson = "ESRIJSON"
    file_gdb = "FileGDB"
    geojson = "GeoJSON"
    geojson_seq = "GeoJSONSeq"
    gpkg = "GPKG"
    shp = "ESRI Shapefile"


class Delimiters(str, Enum):
    __doc__ = "Delimiter used to separate columns in input text file"
    comma = ","
    tab = "\t"
    pipe = "|"
    semicolon = ";"


class IndexType(str, Enum):
    __doc__ = "Index type"
    gist = "gist"
    btree = "btree"
    hash = "hash"


class TileStrategy(str, Enum):
    __doc__ = (
        "Tile strategy for generating vector tiles. "
        "Use `continuous` when working with are mostly adjacent polygon files, "
        "use `discontinuous` when working with polygons feature which are mostly not adjacent"
    )
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
