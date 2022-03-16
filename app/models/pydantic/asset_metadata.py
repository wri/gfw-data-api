from typing import Any, Dict, List, Optional, Type, Union

from pydantic import Field, StrictInt

from ..enum.assets import AssetType
from ..enum.pg_types import PGType
from .base import StrictBaseModel
from .responses import Response


class AssetBase(StrictBaseModel):
    name: str
    asset_type: AssetType


class FieldMetadata(StrictBaseModel):
    field_name_: str = Field(..., alias="field_name")
    field_alias: Optional[str]
    field_description: Optional[str]
    unit: Optional[str]
    field_values: Optional[List[Any]]


class TabularFieldMetadata(FieldMetadata):
    is_feature_info: bool = True
    is_filter: bool = True
    field_type: PGType

    class Config:
        orm_mode = True


class RasterTableRow(StrictBaseModel):
    """
    Mapping of pixel value to what it represents in physical world.
    E.g., in ESA land cover data, 10 represents agriculture use.
    """
    value: int
    meaning: Any


class RasterTable(StrictBaseModel):
    rows: List[RasterTableRow]
    default_meaning: Optional[Any] = None


class RasterBandMetadata(StrictBaseModel):
    # Raster Files/ Raster Tilesets
    pixel_meaning: str
    unit: Optional[str]
    raster_statistics: Optional[Dict[str, Any]]
    raster_table: Optional[RasterTable]
    data_type: Optional[str]
    compression: Optional[str]
    no_data_value: Optional[str]


class RasterTileSetMetadata(AssetBase):
    bands: List[RasterBandMetadata]
    resolution: int


class RasterTileCacheMetadata(AssetBase):
    min_zoom: Optional[int]  # FIXME: Should this really be optional?
    max_zoom: Optional[
        int
    ]  # FIXME: Making required causes exception as it's never set. Find out why
    # TODO: More?


class StaticVectorTileCacheMetadata(AssetBase):
    min_zoom: Optional[int]
    max_zoom: Optional[int]
    # fields_: Optional[List[FieldMetadata]] = Field(None, alias="fields")
    # TODO: default symbology/ legend


class DynamicVectorTileCacheMetadata(StaticVectorTileCacheMetadata):
    min_zoom: StrictInt = 0
    max_zoom: StrictInt = 22


class DatabaseTableMetadata(AssetBase):
    fields_: Optional[List[FieldMetadata]] = Field(None, alias="fields")


class VectorFileMetadata(AssetBase):
    fields_: List[FieldMetadata]


AssetMetadata = Union[
    DatabaseTableMetadata,
    StaticVectorTileCacheMetadata,
    DynamicVectorTileCacheMetadata,
    RasterTileCacheMetadata,
    RasterTileSetMetadata,
    RasterBandMetadata,
    VectorFileMetadata,
]


def asset_metadata_factory(asset_type: str, metadata: Dict[str, Any]) -> AssetMetadata:
    """Create Pydantic Asset Metadata class based on asset type."""
    metadata_factory: Dict[str, Type[AssetMetadata]] = {
        AssetType.static_vector_tile_cache: StaticVectorTileCacheMetadata,
        AssetType.dynamic_vector_tile_cache: DynamicVectorTileCacheMetadata,
        AssetType.raster_tile_cache: RasterTileCacheMetadata,
        AssetType.raster_tile_set: RasterTileSetMetadata,
        AssetType.database_table: DatabaseTableMetadata,
        AssetType.geo_database_table: DatabaseTableMetadata,
        AssetType.ndjson: VectorFileMetadata,
        AssetType.grid_1x1: VectorFileMetadata,
        AssetType.shapefile: VectorFileMetadata,
        AssetType.geopackage: VectorFileMetadata,
    }
    if asset_type in metadata_factory.keys():
        md: AssetMetadata = metadata_factory[asset_type](**metadata)

    else:
        raise NotImplementedError(
            f"Asset metadata factory for type {asset_type} not implemented"
        )

    return md


class FieldMetadataResponse(Response):
    data: Union[List[FieldMetadata], List[TabularFieldMetadata]]
