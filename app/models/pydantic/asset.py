from enum import Enum
from typing import List, Dict, Any, Optional, Union
from uuid import UUID

from pydantic import BaseModel

from .base import Base
from .change_log import ChangeLog
from .metadata import (
    DatabaseTableMetadata,
    VectorTileCacheMetadata,
    RasterTileSetMetadata,
)

AssetMetadata = Union[
    DatabaseTableMetadata, VectorTileCacheMetadata, RasterTileSetMetadata
]


class AssetType(str, Enum):
    source = "Source"
    vector_tile_cache = "Vector tile cache"
    raster_tile_cache = "Raster tile cache"
    raster_tile_set = "Raster tile set (COG/ GeoTIFF)"
    database_table = "Database table"
    shapefile = "Shapefile"
    geopackage = "Geopackage"
    ndjson = "ndjson"
    csv = "csv"
    tsv = "tsv"
    esri_map_service = "ESRI Map Service"
    esri_feature_service = "ESRI Feature Service"
    esri_image_service = "ESRI Image Service"
    esri_vector_service = "ESRI Vector Service"
    arcgis_online_item = "ArcGIS Online item"
    carto_item = "Carto item"
    mapbox_item = "Mapbox item"


class Status(str, Enum):
    failed = "failed"
    pending = "pending"
    success = "success"


class Asset(Base):
    asset_id: UUID
    dataset: str
    version: str
    asset_type: AssetType
    asset_uri: str
    status: Status
    is_managed: bool
    creation_options: Dict[str, Any]
    history: List[Dict[str, Any]]
    metadata: AssetMetadata
    change_log: List[ChangeLog]


class AssetCreateIn(BaseModel):
    asset_type: AssetType
    asset_uri: Optional[str]
    is_managed: bool
    creation_options: Dict[str, Any]
    metadata: Optional[AssetMetadata]


class AssetTaskCreate(BaseModel):
    asset_type: AssetType
    dataset: str
    version: str
    asset_uri: Optional[str]
    is_managed: bool
    creation_options: Dict[str, Any]
    metadata: Optional[AssetMetadata]
