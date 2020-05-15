from typing import List, Optional, Dict, Any, Union

from pydantic import BaseModel, Field
from datetime import date


class FieldMetadata(BaseModel):
    field_name_: str = Field(..., alias="field_name")
    field_alias: Optional[str]
    field_description: Optional[str]
    field_type: str
    is_feature_info: bool = True
    is_filter: bool = True


class DatasetMetadata(BaseModel):
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
    added_date: Optional[str]
    why_added: Optional[str]
    other: Optional[str]
    learn_more: Optional[str]


class VersionMetadata(DatasetMetadata):
    version_number: Optional[str]
    content_date: Optional[str]
    last_update: Optional[date]
    download: Optional[str]
    analysis: Optional[str]
    data_updates: Optional[str]


class RasterTable(BaseModel):
    value: int
    description: str


class RasterTileSetMetadata(VersionMetadata):
    # Raster Files/ Raster Tilesets
    raster_statistics: Optional[Dict[str, Any]]
    raster_table: Optional[List[RasterTable]]
    raster_tiles: Optional[List[str]]
    data_type: Optional[str]
    compression: Optional[str]
    no_data_value: Optional[str]


class VectorTileCacheMetadata(VersionMetadata):
    min_zoom: int
    max_zoom: int


class DatabaseTableMetadata(VersionMetadata):
    fields_: Optional[List[FieldMetadata]] = Field(None, alias="fields")


# class AssetMetadata(VersionMetadata):
#     asset_type: Optional[Dict[str, Any]]
#     url: Optional[str]
#     info: Optional[Union[RasterTileSetMetadata, VectorTileCacheMetadata, FieldMetadata]]
