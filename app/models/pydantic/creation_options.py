from datetime import date
from typing import Any, Dict, List, Optional, Type, Union
from uuid import UUID

from pydantic import Field, root_validator, validator
from pydantic.types import PositiveInt, StrictInt

from ...settings.globals import DEFAULT_JOB_DURATION, PIXETL_DEFAULT_RESAMPLING
from ..enum.assets import AssetType, is_default_asset
from ..enum.creation_options import (
    ConstraintType,
    Delimiters,
    IndexType,
    PartitionType,
    RasterDrivers,
    TableDrivers,
    TileStrategy,
    VectorDrivers,
)
from ..enum.pg_types import PGType
from ..enum.pixetl import (
    DataType,
    Grid,
    NonNumericFloat,
    Order,
    PhotometricType,
    RasterizeMethod,
    ResamplingMethod,
)
from ..enum.sources import (
    RasterSourceType,
    SourceType,
    TableSourceType,
    VectorSourceType,
)
from .base import StrictBaseModel
from .responses import Response
from .symbology import Symbology

COLUMN_REGEX = r"^[a-z][a-zA-Z0-9_-]{2,}$"
PARTITION_SUFFIX_REGEX = r"^[a-z0-9_-]{3,}$"
STR_VALUE_REGEX = r"^[a-zA-Z0-9_-]{1,}$"

NoDataType = Union[StrictInt, NonNumericFloat]


class Index(StrictBaseModel):
    index_type: IndexType
    column_names: List[str] = Field(
        ...,
        description="Columns to be used by index",
        regex=COLUMN_REGEX,
        min_items=1,
        max_items=32,  # A PostgreSQL upper limit
    )


class Constraint(StrictBaseModel):
    constraint_type: ConstraintType
    column_names: List[str] = Field(
        ...,
        description="Columns included in the constraint",
        regex=COLUMN_REGEX,
        min_items=1,
        max_items=32,  # A PostgreSQL upper limit
    )

    class Config:
        orm_mode = True


class HashPartitionSchema(StrictBaseModel):
    partition_count: PositiveInt


class ListPartitionSchema(StrictBaseModel):
    partition_suffix: str = Field(
        ..., description="Suffix for partition table", regex=PARTITION_SUFFIX_REGEX
    )
    value_list: List[str] = Field(
        ..., description="List of values for partition", regex=STR_VALUE_REGEX
    )


class RangePartitionSchema(StrictBaseModel):
    partition_suffix: str = Field(
        ..., description="Suffix for partition table", regex=PARTITION_SUFFIX_REGEX
    )
    start_value: Union[str, int, float, date] = Field(
        ..., description="Start value of partition range", regex=STR_VALUE_REGEX
    )
    end_value: Union[str, int, float, date] = Field(
        ..., description="Start value of partition rang", regex=STR_VALUE_REGEX
    )


class Partitions(StrictBaseModel):
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


class FieldType(StrictBaseModel):
    name: str = Field(..., description="Name of field", regex=COLUMN_REGEX)
    data_type: PGType = Field(..., description="Type of field (PostgreSQL data type).")


class RasterTileSetAssetCreationOptions(StrictBaseModel):
    pixel_meaning: str
    data_type: DataType
    nbits: Optional[int]
    calc: Optional[str]
    band_count: int = 1
    union_bands: bool = False
    no_data: Optional[Union[List[NoDataType], NoDataType]]
    rasterize_method: Optional[RasterizeMethod]
    resampling: ResamplingMethod = PIXETL_DEFAULT_RESAMPLING
    order: Optional[Order]
    overwrite: bool = False
    subset: Optional[str]
    grid: Grid
    symbology: Optional[Symbology] = None
    compute_stats: bool = True
    compute_histogram: bool = False
    process_locally: bool = True
    auxiliary_assets: Optional[List[UUID]] = None
    photometric: Optional[PhotometricType] = None
    num_processes: Optional[StrictInt] = None
    timeout_sec: Optional[StrictInt] = Field(
        None,
        description="Maximum run time for associated AWS Batch jobs, in seconds",
    )

    @validator("no_data")
    def validate_no_data(cls, v, values, **kwargs):
        if isinstance(v, list):
            assert len(v) == int(
                values.get("band_count")
            ), f"Length of no data ({v}) list must match band count ({values.get('band_count')})."
            assert (
                len(set(v)) == 1
            ), "No data values must be the same for all bands"  # RasterIO does not support different no data values for bands
        return v


class PixETLCreationOptions(RasterTileSetAssetCreationOptions):
    # For internal use only
    source_type: Union[RasterSourceType, VectorSourceType]
    source_driver: Optional[RasterDrivers] = None
    source_uri: Optional[List[str]] = Field(
        description="List of input sources. Sources must be the URI of either a "
        "tiles.geojson file on S3 or a folder (prefix) on S3 or GCS. "
        "Features in tiles.geojson must have path starting with either /vsis3/ or /vsigs/",
    )

    @validator("source_uri")
    def validate_source_uri(cls, v, values, **kwargs):
        if values.get("source_type") == SourceType.raster:
            assert v, "Raster source types require source_uri"
        else:
            assert not v, "Only raster source type require source_uri"
        return v


class RasterTileSetSourceCreationOptions(PixETLCreationOptions):
    # Keep source_type and source_driver mandatory without default value
    # This will help Pydantic to differentiate between
    # RasterTileSetSourceCreationOptions and RasterTileSetAssetCreationOptions
    source_type: RasterSourceType = Field(..., description="Source type of input file.")
    source_driver: RasterDrivers = Field(
        ...,
        description="Driver of source file. Must be a GDAL driver",
    )


class VectorSourceCreationOptions(StrictBaseModel):
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
            Index(index_type=IndexType.gist.value, column_names=["geom"]),
            Index(index_type=IndexType.gist.value, column_names=["geom_wm"]),
            Index(index_type=IndexType.hash.value, column_names=["gfw_geostore_id"]),
        ],
        description="List of indices to add to table",
    )
    cluster: Optional[Index] = Field(None, description="Index to use for clustering.")
    table_schema: Optional[List[FieldType]] = Field(
        None,
        description="List of Field Types. Missing field types will be inferred. (optional)",
    )
    create_dynamic_vector_tile_cache: bool = Field(
        True,
        description=(
            "By default, vector sources will implicitly create a dynamic vector tile cache. "
            "Disable this option by setting value to `false`"
        ),
    )
    add_to_geostore: bool = Field(
        True,
        description="Make geometries searchable via geostore endpoint.",
    )
    timeout: int = DEFAULT_JOB_DURATION

    @validator("source_uri")
    def validate_source_uri(cls, v, values, **kwargs):
        if values.get("source_driver") == VectorDrivers.csv:
            assert len(v) >= 1, "CSV sources require at least one input file"
        else:
            assert (
                len(v) == 1
            ), "Non-CSV vector sources require one and only one input file"
        return v


class TableAssetCreationOptions(StrictBaseModel):
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
    constraints: Optional[List[Constraint]] = Field(
        None, description="List of constraints to add to table. (optional)"
    )
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
    timeout: int = DEFAULT_JOB_DURATION

    @validator("constraints")
    def validate_max_1_unique_constraints(cls, v, values, **kwargs):
        if v is not None:
            unique_constraints = [
                c for c in v if c.constraint_type == ConstraintType.unique
            ]
            assert (
                len(unique_constraints) < 2
            ), "Currently cannot specify more than 1 unique constraint"
        return v


class TableSourceCreationOptions(TableAssetCreationOptions):
    source_type: TableSourceType = Field(..., description="Source type of input file.")
    source_driver: TableDrivers
    source_uri: List[str] = Field(
        ..., description="List of input files. Must be a list of s3:// urls"
    )


class TileCacheBaseModel(StrictBaseModel):
    min_zoom: int = Field(
        0, description="Minimum zoom level of tile cache", ge=0, le=22
    )
    max_zoom: int = Field(
        14, description="Maximum zoom level of tile cache", ge=0, le=22
    )
    timeout_sec: Optional[StrictInt] = Field(
        None,
        description="Maximum run time for associated AWS Batch jobs, in seconds",
    )

    @classmethod
    @root_validator(allow_reuse=True)
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
    symbology: Symbology = Field(..., description="Symbology to use for output tiles")
    source_asset_id: str = Field(
        ...,
        description="Raster tile set asset ID to use as source. "
        "Must be an asset of the same dataset version",
    )
    resampling: ResamplingMethod = Field(
        ResamplingMethod.average,
        description="Resampling method used to downsample tiles",
    )


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


class StaticVectorFileCreationOptions(StrictBaseModel):
    field_attributes: Optional[List[str]] = Field(
        None,
        description="Field attributes to include in vector tiles. "
        "If left blank, all fields marked as `is_feature_info` will be included.",
    )


SourceCreationOptions = Union[
    TableSourceCreationOptions,
    RasterTileSetSourceCreationOptions,
    VectorSourceCreationOptions,
]

OtherCreationOptions = Union[
    TableAssetCreationOptions,
    RasterTileCacheCreationOptions,
    StaticVectorTileCacheCreationOptions,
    StaticVectorFileCreationOptions,
    DynamicVectorTileCacheCreationOptions,
    RasterTileSetAssetCreationOptions,
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
