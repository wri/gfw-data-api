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


class ConstraintType(str, Enum):
    __doc__ = "Constraint type"
    unique = "unique"


class TileStrategy(str, Enum):
    __doc__ = (
        "Tile strategy for generating vector tiles. "
        "Use `continuous` when working with mostly adjacent polygon files, "
        "use `discontinuous` when working with polygon features which are mostly non-adjacent"
        "Use `keep_all` if you don't want features to be removed at all. This might lead to larger tiles."
    )
    continuous = "continuous"
    discontinuous = "discontinuous"
    keep_all = "keep_all"


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


class ColorMapType(str, Enum):
    discrete = "discrete"
    discrete_intensity = "discrete_intensity"
    gradient = "gradient"
    gradient_intensity = "gradient_intensity"
    date_conf_intensity = "date_conf_intensity"
    date_conf_intensity_multi_8 = "date_conf_intensity_multi_8"
    date_conf_intensity_multi_16 = "date_conf_intensity_multi_16"
    year_intensity = "year_intensity"
    value_intensity = "value_intensity"
