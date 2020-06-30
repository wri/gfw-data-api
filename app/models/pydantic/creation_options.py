from datetime import date
from typing import Any, Dict, List, Optional, Type, Union

from pydantic import BaseModel, Field
from pydantic.types import PositiveInt

from ..enum.assets import AssetType
from ..enum.creation_options import (
    Delimiters,
    IndexType,
    PartitionType,
    PGType,
    TableDrivers,
    TileStrategy,
    VectorDrivers,
)
from ..enum.sources import SourceType

COLUMN_REGEX = r"^[a-z][a-zA-Z0-9_-]{2,}$"
PARTITION_SUFFIX_REGEX = r"^[a-z0-9_-]{3,}$"
STR_VALUE_REGEX = r"^[a-zA-Z0-9_-]{1,}$"


class Index(BaseModel):
    index_type: IndexType
    column_name: str = Field(
        ..., description="Column to be used by index", regex=COLUMN_REGEX
    )


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


class NdjsonCreationOptions(BaseModel):
    pass


SourceCreationOptions = Union[VectorSourceCreationOptions, TableSourceCreationOptions]

OtherCreationOptions = Union[
    StaticVectorTileCacheCreationOptions, NdjsonCreationOptions
]

CreationOptions = Union[SourceCreationOptions, OtherCreationOptions]


def asset_creation_option_factory(
    source_type: Optional[str], asset_type: str, creation_options: Dict[str, Any]
) -> CreationOptions:
    """Create Asset Creation Option based on asset or source type."""

    source_creation_option_factory: Dict[str, Type[SourceCreationOptions]] = {
        SourceType.vector: VectorSourceCreationOptions,
        SourceType.table: TableSourceCreationOptions,
        # SourceType.raster: RasterSourceCreationOptions
    }

    creation_options_factory: Dict[str, Type[OtherCreationOptions]] = {
        AssetType.static_vector_tile_cache: StaticVectorTileCacheCreationOptions,
        AssetType.ndjson: NdjsonCreationOptions,
    }

    try:
        if (
            asset_type == AssetType.database_table
            or asset_type == AssetType.raster_tile_set
        ) and source_type:
            co: CreationOptions = source_creation_option_factory[source_type](
                **creation_options
            )
        else:
            co = creation_options_factory[asset_type](**creation_options)
    except KeyError:
        raise NotImplementedError(
            f"Asset creation options factory for type {asset_type} and source {source_type} not implemented"
        )

    return co
