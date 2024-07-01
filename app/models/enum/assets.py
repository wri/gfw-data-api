from enum import Enum
from typing import Any, Dict

from app.models.enum.sources import SourceType


class AssetStatus(str, Enum):
    saved = "saved"
    pending = "pending"
    failed = "failed"


class AssetType(str, Enum):
    dynamic_vector_tile_cache = "Dynamic vector tile cache"
    static_vector_tile_cache = "Static vector tile cache"
    raster_tile_cache = "Raster tile cache"
    raster_tile_set = "Raster tile set"
    database_table = "Database table"
    geo_database_table = "Geo database table"
    shapefile = "ESRI Shapefile"
    geopackage = "Geopackage"
    ndjson = "ndjson"
    csv = "csv"
    tsv = "tsv"
    grid_1x1 = "1x1 grid"
    # esri_map_service = "ESRI Map Service"
    # esri_feature_service = "ESRI Feature Service"
    # esri_image_service = "ESRI Image Service"
    # esri_vector_service = "ESRI Vector Service"
    # arcgis_online_item = "ArcGIS Online item"
    # carto_item = "Carto item"
    # mapbox_item = "Mapbox item"


def default_asset_type(source_type: str, creation_option: Dict[str, Any]) -> str:
    """Get default asset type based on source type and creation options."""

    lat = creation_option.get("latitude", None)
    lng = creation_option.get("longitude", None)

    if source_type == SourceType.vector:
        asset_type = AssetType.geo_database_table
    elif source_type == SourceType.table and lat and lng:
        asset_type = AssetType.geo_database_table
    elif source_type == SourceType.table:
        asset_type = AssetType.database_table
    elif source_type == SourceType.raster:
        asset_type = AssetType.raster_tile_set
    else:
        raise NotImplementedError("Not a supported input source")
    return asset_type


def is_database_asset(asset_type: str) -> bool:
    return asset_type in [AssetType.geo_database_table, AssetType.database_table]


def is_single_file_asset(asset_type: str) -> bool:
    return asset_type in [
        AssetType.geopackage,
        AssetType.ndjson,
        AssetType.shapefile,
        AssetType.csv,
        AssetType.tsv,
        AssetType.grid_1x1,
    ]


def is_tile_cache_asset(asset_type: str) -> bool:
    return asset_type in [
        AssetType.dynamic_vector_tile_cache,
        AssetType.static_vector_tile_cache,
        AssetType.raster_tile_cache,
    ]


def is_default_asset(asset_type: str) -> bool:
    return asset_type in [
        AssetType.database_table,
        AssetType.raster_tile_set,
        AssetType.geo_database_table,
    ]
