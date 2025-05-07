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
    TileBlockSize,
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
    unify_projection: bool = Field(
        False,
        description=(
            "First re-project to a common projection (EPSG:4326). Necessary "
            "when input files are in different projections from each other."
        )
    )
    pixel_meaning: str = Field(
        ..., description="Description of what the pixel value in the "
        "raster represents. This is used to clarify the meaning of the raster "
        "and distinguish multiple raster tile sets based on the same dataset "
        "version. The pixel_meaning string should be fairly short, use all "
        "lower-case letters, and use underscores instead of spaces."
    )
    data_type: DataType = Field(
        ..., description=("The type of the data stored at every pixel of "
                          "the destination raster")
    )
    nbits: Optional[int] = Field(
        None,
        description="Advanced option that lets GDAL compress the data even "
        "more based on the number of bits you need."
    )
    calc: Optional[str] = Field(
        None,
        description="There are two modes for this field, one for rasterizing vector "
        "sources and one for transforming and/or combining one or more "
        "sources that are already raster. For rasterizing vector sources, "
        "this field should be an SQL expression that yields the desired "
        "raster value based on the fields of your vector dataset.\n\nFor raster "
        "sources, this should be a raster algebra expression, similar to that "
        "provided to gdal_calc (see "
        "https://gdal.org/en/stable/programs/gdal_calc.html), "
        "that transforms one or more input bands into one or more output "
        "bands. For use in this expression, each band in "
        "the sources is assigned an alphabetic variable (A-Z, then AA-AZ, "
        "etc.) in the order it exists in those sources, with those of the "
        "first source first, continuing with those of the second, and so on. "
        "So with two input sources of two bands each, they would be assigned "
        "to variables A and B (for the first source) and C and D (for the "
        "second source). The NumPy module is in scope, accessible as np"
    )
    band_count: int = Field(
        1,
        description=(
            "The number of bands in the output raster.  The default is 1, and "
            "output rasters with multiple bands is not common. To create multiple "
            "bands in the output, the calc string will normally use "
            "np.ma.array([...])."
        )
    )
    union_bands: bool = Field(
        False,
        description=(
            "Relevant only for multiple input layers (because of multiple bands or "
            "auxiliary assets).  If true, then the destination extent is the union of "
            "the extents of the source layers. This is useful when some of the input "
            "bands have limited geographic extents. If false (the default), then the "
            "destination extent is the intersection of the extents of the source layers."
        )
    )
    no_data: Optional[Union[List[NoDataType], NoDataType]] = Field(
        None,
        description=(
            "The value of a pixel that indicates that no data is present (because the "
            "dataset extent does not include that pixel). Typical values are -1 for signed "
            "ints, 0 for unsigned ints, and nan for floating point values. But any valid "
            "value of the data type can be used. If nodata is a List, its length must "
            "be equal to band_count."
        )
    )
    rasterize_method: Optional[RasterizeMethod] = Field(
        RasterizeMethod.value,
        description="For raster sources or default assets, 'value' (the "
        "default) means use the value from the last or only band processed, "
        "and 'count' means count the number of bands with data values."
    )
    resampling: ResamplingMethod = PIXETL_DEFAULT_RESAMPLING
    order: Optional[Order] = Field(
        None,
        description="For vector default assets, order the features by the "
        "calculated raster value. For 'asc', the features are ordered by "
        "ascending calculated value so that the largest calculated value is "
        "used in the raster when there are overlapping features. For 'desc', "
        "the ordering is descending, so that the smallest calculated value "
        "is used when there are overlaps."
    )
    overwrite: bool = False
    subset: Optional[str]
    grid: Grid
    symbology: Optional[Symbology] = None
    compute_stats: bool = True
    compute_histogram: bool = False
    process_locally: bool = True
    auxiliary_assets: Optional[List[UUID]] = Field(
        None,
        description="Asset IDs of additional rasters you might want to include "
        "in your calc expression."
    )
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
        description="List of indices to add to the database table representing "
        "the vector dataset.  Each element of the indices field contains an "
        "index_type field (which is a string) and a column_names field (which "
        "is a list of field names included in this index). The possibilities "
        "for the index_type field are hash, btree, or gist. hash is efficient "
        "for standard exact-value lookups, while btree is efficient for range "
        "lookups. gist is used for geometry fields and can do "
        "intersection-type lookups. See "
        "https://www.postgresql.org/docs/current/indexes-types.html"
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
        elif values.get("source_driver") in [VectorDrivers.esrijson, VectorDrivers.shp, VectorDrivers.geojson_seq, VectorDrivers.geojson]:
            assert (len(v) == 1), "GeoJSON and ESRI Shapefile vector sources require one and only one input file"
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
        "allow creation of multiple raster tile caches per version,",
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


class COGCreationOptions(StrictBaseModel):
    implementation: str = Field(
        "default",
        description="Name space to use for COG. "
        "This will be part of the URI and will "
        "allow creation of multiple COGs per version.",
    )
    source_asset_id: str = Field(
        ...,
        description="Raster tile set asset ID to use as source. "
        "Must be an asset of the same version",
    )
    resampling: ResamplingMethod = Field(
        ResamplingMethod.average,
        description="Resampling method used to downsample overviews",
    )
    block_size: Optional[TileBlockSize] = Field(
        512,
        description="Block size to tile COG with.",
    )
    compute_stats: bool = False
    export_to_gee: bool = Field(
        False,
        description="Option to export COG to a Google Cloud Storage and create"
        " a COG-backed asset on Google Earth Engine (GEE). The asset will be created"
        " under the project `forma-250` with the asset ID `{dataset}/{implementation}. "
        "Versioning is currently not supported due to GEE storage constraints.",
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
    feature_filter: Optional[Dict[str, Any]] = Field(
        None,
        description="Optional tippecanoe feature filter(s). Uses the syntax of "
        "[Mapbox legacy filters](https://docs.mapbox.com/style-spec/reference/other/#other-filters)"
    )


class StaticVectorFileCreationOptions(StrictBaseModel):
    field_attributes: Optional[List[str]] = Field(
        None,
        description="Field attributes to include in vector tiles. "
        "If left blank, all fields marked as `is_feature_info` will be included.",
    )


class StaticVector1x1CreationOptions(StaticVectorFileCreationOptions):
    include_tile_id: Optional[bool] = Field(
        False, description="Whether or not to include the tile_id of each feature"
    )


SourceCreationOptions = Union[
    TableSourceCreationOptions,
    RasterTileSetSourceCreationOptions,
    VectorSourceCreationOptions,
]

OtherCreationOptions = Union[
    TableAssetCreationOptions,
    COGCreationOptions,
    RasterTileCacheCreationOptions,
    StaticVectorTileCacheCreationOptions,
    StaticVectorFileCreationOptions,
    StaticVector1x1CreationOptions,
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
    AssetType.grid_1x1: StaticVector1x1CreationOptions,
    AssetType.shapefile: StaticVectorFileCreationOptions,
    AssetType.geopackage: StaticVectorFileCreationOptions,
    AssetType.raster_tile_set: RasterTileSetAssetCreationOptions,
    AssetType.cog: COGCreationOptions,
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
