from enum import Enum

from .base import Base
from .metadata import Metadata


class AssetType(str, Enum):
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
    external = "external"


class Asset(Base):
    type: str = "version"
    dataset: str
    version: str
    asset_type: bool
    asset_uri: str
    # status: str
    metadata: Metadata