from datetime import date
from typing import Any, Dict, List, Optional, Type, Union

from pydantic import BaseModel, Field, root_validator
from pydantic.types import PositiveInt

from ..enum.assets import AssetType, is_default_asset
from ..enum.creation_options import (
    Delimiters,
    IndexType,
    PartitionType,
    RasterDrivers,
    TableDrivers,
    TileStrategy,
    VectorDrivers,
)
from ..enum.pg_types import PGType
from ..enum.pixetl import DataType, Grid, Order, RasterizeMethod, ResamplingMethod
from ..enum.sources import (
    RasterSourceType,
    SourceType,
    TableSourceType,
    VectorSourceType,
)
from .responses import Response
from .symbology import Symbology

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


class RasterTileSetAssetCreationOptions(BaseModel):
    pixel_meaning: str
    data_type: DataType
    nbits: Optional[int]
    no_data: Optional[int]
    rasterize_method: Optional[RasterizeMethod]
    resampling: Optional[ResamplingMethod]
    calc: Optional[str]
    order: Optional[Order]
    overwrite: Optional[bool]
    subset: Optional[str]
    grid: Grid

    class Config:
        extra = "forbid"


class RasterTileSetSourceCreationOptions(RasterTileSetAssetCreationOptions):
    source_type: RasterSourceType = Field(..., description="Source type of input file.")
    source_driver: RasterDrivers = Field(
        ..., description="Driver of source file. Must be an OGR driver"
    )
    source_uri: Optional[List[str]] = Field(
        description="List of input files. Must be s3:// URLs.",
    )


class VectorSourceCreationOptions(BaseModel):
    source_type: VectorSourceType = Field(..., description="Source type of input file.")
    source_driver: VectorDrivers = Field(
        ..., description="Driver of source file. Must be an OGR driver"
    )
    source_uri: List[str] = Field(
        ...,
        description="List of input files. Vector source layers can only have one list item. "
        "Must be a s3:// url.",
    )
    layers: Optional[List[str]] = Field(
        None, description="List of input layers. Only required for .gdb and .gpkg."
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
    add_to_geostore: bool = Field(
        True,
        description="Include features to geostore, to make geometries searchable via geostore endpoint.",
    )

    class Config:
        extra = "forbid"


class TableAssetCreationOptions(BaseModel):
    has_header: bool = Field(True, description="Input file has header. Must be true")
    delimiter: Delimiters = Field(..., description="Delimiter used in input file")

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
        "when geographic columns are present. "
        "Disable this option by setting value to `false`",
    )

    class Config:
        extra = "forbid"


class TableSourceCreationOptions(TableAssetCreationOptions):
    source_type: TableSourceType = Field(..., description="Source type of input file.")
    source_driver: TableDrivers
    source_uri: List[str] = Field(
        ..., description="List of input files. Must be a list of s3:// urls"
    )


class TileCacheBaseModel(BaseModel):
    min_zoom: int = Field(
        0, description="Minimum zoom level of tile cache", ge=0, le=22
    )
    max_zoom: int = Field(
        22, description="Maximum zoom level of tile cache", ge=0, le=22
    )

    @classmethod
    @root_validator
    def check_zoom(cls, values):
        min_zoom, max_zoom, max_static_zoom = (
            values.get("min_zoom", None),
            values.get("max_zoom", None),
            values.get("max_static_zoom", None),
        )
        if (min_zoom and max_zoom) and (max_zoom < min_zoom):
            raise ValueError("`max_zoom` must be equal or larger than `min_zoom`")
        if (min_zoom and max_static_zoom) and (max_static_zoom < min_zoom):
            raise ValueError(
                "`max_static_zoom` must be equal or larger than `min_zoom`"
            )
        if (max_zoom and max_static_zoom) and (max_zoom < max_static_zoom):
            raise ValueError(
                "`max_zoom` must be equal or larger than `max_static_zoom`"
            )
        return values

    class Config:
        extra = "forbid"


class RasterTileCacheCreationOptions(TileCacheBaseModel):
    # FIXME: Should we make the max_static_zoom upper limit lower to avoid DOS?
    max_static_zoom: int = Field(
        9, description="Maximum zoom level to pre-generate tiles for", ge=0, le=22
    )
    implementation: str = Field(
        "default",
        description="Name space to use for raster tile cache. "
        "This will be part of the URI and will "
        "allow to create multiple raster tile caches per version,",
    )
    symbology: Symbology
    source_asset_id: str


class DynamicVectorTileCacheCreationOptions(TileCacheBaseModel):
    field_attributes: Optional[List[Dict[str, Any]]] = Field(
        None,
        description="Field attributes to include in vector tiles. "
        "If left blank, all fields marked as `is_feature_info` will be included.",
    )


class StaticVectorTileCacheCreationOptions(TileCacheBaseModel):
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
    implementation: str = Field(
        "default",
        description="Name space to use for static tile cache. "
        "This will be part of the URI and will "
        "allow to create multiple static tile caches per version,",
    )
    layer_style: List[Dict[str, Any]] = Field(
        ...,
        description="List of [Mapbox layer styling rules](https://docs.mapbox.com/mapbox-gl-js/style-spec/layers) "
        "for vector tile caches. `source` and `source-layer` attributes must use `dataset` name."
        "Styling rules will be used in autogenerated root.json and preview.",
    )


class StaticVectorFileCreationOptions(BaseModel):
    field_attributes: Optional[List[str]] = Field(
        None,
        description="Field attributes to include in vector tiles. "
        "If left blank, all fields marked as `is_feature_info` will be included.",
    )

    class Config:
        extra = "forbid"


SourceCreationOptions = Union[
    RasterTileSetSourceCreationOptions,
    TableSourceCreationOptions,
    VectorSourceCreationOptions,
]

OtherCreationOptions = Union[
    StaticVectorTileCacheCreationOptions,
    StaticVectorFileCreationOptions,
    RasterTileCacheCreationOptions,
    DynamicVectorTileCacheCreationOptions,
    RasterTileSetAssetCreationOptions,
    TableAssetCreationOptions,
]

CreationOptions = Union[SourceCreationOptions, OtherCreationOptions]


class CreationOptionsResponse(Response):
    data: CreationOptions


SourceCreationOptionsLookup: Dict[str, Type[SourceCreationOptions]] = {
    SourceType.vector: VectorSourceCreationOptions,
    SourceType.table: TableSourceCreationOptions,
    SourceType.raster: RasterTileSetSourceCreationOptions,
}

AssetCreationOptionsLookup: Dict[str, Type[OtherCreationOptions]] = {
    AssetType.dynamic_vector_tile_cache: DynamicVectorTileCacheCreationOptions,
    AssetType.static_vector_tile_cache: StaticVectorTileCacheCreationOptions,
    AssetType.ndjson: StaticVectorFileCreationOptions,
    AssetType.grid_1x1: StaticVectorFileCreationOptions,
    AssetType.shapefile: StaticVectorFileCreationOptions,
    AssetType.geopackage: StaticVectorFileCreationOptions,
    AssetType.raster_tile_set: RasterTileSetAssetCreationOptions,
    AssetType.raster_tile_cache: RasterTileCacheCreationOptions,
    AssetType.database_table: TableAssetCreationOptions,
}


def creation_option_factory(
    asset_type: str, creation_options: Dict[str, Any]
) -> CreationOptions:
    """Create Asset Creation Option based on asset or source type."""

    source_type = creation_options.get("source_type", None)

    try:
        if is_default_asset(asset_type) and source_type:
            co: CreationOptions = SourceCreationOptionsLookup[source_type](
                **creation_options
            )
        else:
            co = AssetCreationOptionsLookup[asset_type](**creation_options)
    except KeyError:
        raise NotImplementedError(
            f"Asset creation options factory for type {asset_type} and source {source_type} not implemented"
        )

    return co
