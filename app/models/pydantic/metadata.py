from typing import Any, Dict, List, Optional, Type, Union

from pydantic import Extra, Field, StrictInt

from ..enum.assets import AssetType
from ..enum.pg_types import PGType
from .base import StrictBaseModel
from .responses import Response


class FieldMetadata(StrictBaseModel):
    field_name_: str = Field(..., alias="field_name")
    field_alias: Optional[str]
    field_description: Optional[str]
    field_type: PGType
    is_feature_info: bool = True
    is_filter: bool = True

    class Config:
        orm_mode = True
        extra = Extra.forbid


class DatasetMetadata(StrictBaseModel):
    title: Optional[str]
    subtitle: Optional[str]
    function: Optional[str]
    resolution: Optional[str]
    geographic_coverage: Optional[str]
    source: Optional[str]
    update_frequency: Optional[str]
    cautions: Optional[str]
    license: Optional[str]
    overview: Optional[str]
    citation: Optional[str]
    tags: Optional[List[str]]
    data_language: Optional[str]
    key_restrictions: Optional[str]
    scale: Optional[str]
    added_date: Optional[str] = Field(
        None,
        description="Date the data were added to GFW website",
        regex="^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])$",
    )
    why_added: Optional[str]
    other: Optional[str]
    learn_more: Optional[str]


class VersionMetadata(DatasetMetadata):
    version_number: Optional[str]
    content_date: Optional[str] = Field(
        None,
        description="Date content was created",
        regex="^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])$",
    )
    last_update: Optional[str] = Field(
        None,
        description="Date the data were last updated",
        regex="^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])$",
    )
    download: Optional[str]
    analysis: Optional[str]
    data_updates: Optional[str]


class RasterTable(StrictBaseModel):
    value: int
    description: str


class RasterTileCacheMetadata(VersionMetadata):
    min_zoom: Optional[int]  # FIXME: Should this really be optional?
    max_zoom: Optional[
        int
    ]  # FIXME: Making required causes exception as it's never set. Find out why
    # TODO: More?


class RasterTileSetMetadata(VersionMetadata):
    # Raster Files/ Raster Tilesets
    raster_statistics: Optional[Dict[str, Any]]
    raster_table: Optional[List[RasterTable]]
    raster_tiles: Optional[List[str]]
    data_type: Optional[str]
    compression: Optional[str]
    no_data_value: Optional[str]


class StaticVectorTileCacheMetadata(VersionMetadata):
    min_zoom: Optional[int]
    max_zoom: Optional[int]
    # fields_: Optional[List[FieldMetadata]] = Field(None, alias="fields")
    # TODO: default symbology/ legend


class DynamicVectorTileCacheMetadata(StaticVectorTileCacheMetadata):
    min_zoom: StrictInt = 0
    max_zoom: StrictInt = 22


class DatabaseTableMetadata(VersionMetadata):
    # fields_: Optional[List[FieldMetadata]] = Field(None, alias="fields")
    pass


class VectorFileMetadata(VersionMetadata):
    pass


AssetMetadata = Union[
    DatabaseTableMetadata,
    StaticVectorTileCacheMetadata,
    DynamicVectorTileCacheMetadata,
    RasterTileCacheMetadata,
    RasterTileSetMetadata,
    VectorFileMetadata,
]


class FieldMetadataResponse(Response):
    data: List[FieldMetadata]


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
