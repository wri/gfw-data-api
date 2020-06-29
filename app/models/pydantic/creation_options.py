from datetime import date
from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel, Field
from pydantic.types import PositiveInt

COLUMN_REGEX = r"^[a-z][a-zA-Z0-9_-]{2,}$"
PARTITION_SUFFIX_REGEX = r"^[a-z0-9_-]{3,}$"
STR_VALUE_REGEX = r"^[a-zA-Z0-9_-]{1,}$"


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


class Index(BaseModel):
    index_type: IndexType
    column_name: str = Field(
        ..., description="Column to be used by index", regex=COLUMN_REGEX
    )


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
    integer = "integer"
    jsonb = "jsonb"
    numeric = "numeric"
    smallint = "smallint"
    text = "text"
    time = "time"
    timestamp = "timestamp"
    uuid = "uuid"
    xml = "xml"


class HashPartitionSchema(BaseModel):
    partition_count: PositiveInt


class ListPartitionSchema(BaseModel):
    partition_suffix: str = Field(
        ..., description="Suffix for partition table", regex=PARTITION_SUFFIX_REGEX
    )
    value_list: List[str] = Field(
        ..., description="List of values for partition", regex=STR_VALUE_REGEX
    )


class RangePartitionSchema(BaseModel):
    partition_suffix: str = Field(
        ..., description="Suffix for partition table", regex=PARTITION_SUFFIX_REGEX
    )
    start_value: Union[str, int, float, date] = Field(
        ..., description="Start value of partition range", regex=STR_VALUE_REGEX
    )
    end_value: Union[str, int, float, date] = Field(
        ..., description="Start value of partition rang", regex=STR_VALUE_REGEX
    )


class Partitions(BaseModel):
    partition_type: PartitionType = Field(..., description="Partition type")
    partition_column: str = Field(
        ..., description="Column to be used to create partitions.", regex=COLUMN_REGEX
    )
    create_default: bool = Field(
        False,
        description="Create default partition to cache values not captured by partition schema",
    )
    partition_schema: Union[
        HashPartitionSchema, List[ListPartitionSchema], List[RangePartitionSchema]
    ] = Field(..., description="Partition Schema to be used.")


class FieldType(BaseModel):
    field_name: str = Field(..., description="Name of field", regex=COLUMN_REGEX)
    field_type: PGType = Field(..., description="Type of field (PostgreSQL type).")


class VectorSourceCreationOptions(BaseModel):
    src_driver: VectorDrivers = Field(
        ..., description="Driver of source file. Must be an OGR driver"
    )
    zipped: bool = Field(..., description="Indicate if source file is zipped")
    layers: Optional[List[str]] = Field(
        None, description="List of input layers. Only required for .gdb and .gpkg"
    )
    indices: List[Index] = Field(
        [
            Index(index_type="gist", column_name="geom"),
            Index(index_type="gist", column_name="geom_wm"),
            Index(index_type="hash", column_name="gfw_geostore_id"),
        ],
        description="List of indices to add to table",
    )
    create_dynamic_vector_tile_cache: bool = Field(
        True,
        description="By default, vector sources will implicitly create a dynamic vector tile cache. "
        "Disable this option by setting value to `false`",
    )


class TableSourceCreationOptions(BaseModel):
    src_driver: TableDrivers = Field(..., description="Driver of input file.")
    delimiter: Delimiters = Field(..., description="Delimiter used in input file")
    has_header: bool = Field(True, description="Input file has header. Must be true")
    latitude: Optional[str] = Field(
        None, description="Column with latitude coordinate", regex=COLUMN_REGEX
    )
    longitude: Optional[str] = Field(
        None, description="Column with longitude coordinate", regex=COLUMN_REGEX
    )
    cluster: Optional[Index] = Field(
        None, description="Index to use for clustering (optional)."
    )
    partitions: Optional[Partitions] = Field(
        None, description="Partitioning schema (optional)"
    )
    indices: List[Index] = Field([], description="List of indices to add to table")
    table_schema: Optional[List[FieldType]] = Field(
        None,
        description="List of Field Types. Missing field types will be inferred. (optional)",
    )
    create_dynamic_vector_tile_cache: bool = Field(
        True,
        description="By default, table sources will implicitly create a dynamic vector tile cache "
        "when geographic columns are present"
        "Disable this option by setting value to `false`",
    )


class DynamicVectorTileCacheCreationOptions(BaseModel):
    min_zoom: int = Field(
        0, description="Minimum zoom level of tile cache", ge=0, le=22
    )
    max_zoom: int = Field(
        22, description="Maximum zoom level of tile cache", ge=0, le=22
    )


class StaticVectorTileCacheCreationOptions(BaseModel):
    min_zoom: int = Field(
        ..., description="Minimum zoom level of tile cache", ge=0, le=22
    )
    max_zoom: int = Field(
        ..., description="Maximum zoom level of tile cache", ge=0, le=22
    )
    field_attributes: Optional[List[str]] = Field(
        None,
        description="Field attributes to include in vector tiles. "
        "If left blank, all fields marked as `is_feature_info` will be included.",
    )

    tile_strategy: TileStrategy = Field(
        ...,
        description="`discontinuous` corresponds to `drop-densest-as-needed` and"
        "`continuous` corresponds to `coalesce-densest-as-needed`",
    )


CreationOptions = Union[
    VectorSourceCreationOptions,
    TableSourceCreationOptions,
    StaticVectorTileCacheCreationOptions,
]
