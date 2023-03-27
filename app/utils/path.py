import os
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from botocore.exceptions import ClientError

from app.models.enum.assets import AssetType
from app.settings.globals import API_URL, DATA_LAKE_BUCKET, TILE_CACHE_URL
from app.utils.aws import get_s3_client


def split_s3_path(s3_path: str) -> Tuple[str, str]:
    o = urlparse(s3_path, allow_fragments=False)
    return o.netloc, o.path.lstrip("/")


def infer_srid_from_grid(grid: str) -> str:
    if grid.startswith("zoom_"):
        return "epsg-3857"
    return "epsg-4326"


def is_zipped(s3_uri: str) -> bool:
    """Determine from file name or header if URI points to a Zip file."""
    _, ext = os.path.splitext(s3_uri)
    if ext.lower() == ".zip":
        return True

    client = get_s3_client()
    bucket, key = split_s3_path(s3_uri)
    try:
        header = client.head_object(Bucket=bucket, Key=key)
        if header["ContentType"] == "application/x-zip-compressed":
            return True
    except (KeyError, ClientError):
        raise FileNotFoundError(f"Cannot access source file {s3_uri}")

    return False


def get_layer_name(uri):
    name, ext = os.path.splitext(os.path.basename(uri))
    if ext == "":
        return name
    else:
        return get_layer_name(name)


def get_asset_uri(
    dataset: str,
    version: str,
    asset_type: str,
    creation_options: Optional[Dict[str, Any]] = None,
    srid: str = "epsg:4326",
) -> str:

    srid = srid.replace(":", "-")

    if not creation_options:
        creation_options = {}

    grid = creation_options.get("grid", None)
    value = creation_options.get("pixel_meaning", None)
    implementation = creation_options.get("implementation", "default")

    uri_constructor: Dict[str, str] = {
        AssetType.dynamic_vector_tile_cache: f"{TILE_CACHE_URL}/{dataset}/{version}/dynamic/{{z}}/{{x}}/{{y}}.pbf",
        AssetType.static_vector_tile_cache: f"{TILE_CACHE_URL}/{dataset}/{version}/{implementation}/{{z}}/{{x}}/{{y}}.pbf",
        AssetType.raster_tile_cache: f"{TILE_CACHE_URL}/{dataset}/{version}/{implementation}/{{z}}/{{x}}/{{y}}.png",
        AssetType.shapefile: f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/vector/{srid}/{dataset}_{version}.shp.zip",
        AssetType.ndjson: f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/vector/{srid}/{dataset}_{version}.ndjson",
        AssetType.grid_1x1: f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/vector/{srid}/{dataset}_{version}_1x1.tsv",
        AssetType.geopackage: f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/vector/{srid}/{dataset}_{version}.gpkg",
        AssetType.csv: f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/text/{dataset}_{version}.csv",
        AssetType.tsv: f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/text/{dataset}_{version}.tsv",
        AssetType.geo_database_table: f"{API_URL}/dataset/{dataset}/{version}/query",
        AssetType.database_table: f"{API_URL}/dataset/{dataset}/{version}/query",
        AssetType.raster_tile_set: f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/raster/{srid}/{grid}/{value}/geotiff/{{tile_id}}.tif",
    }

    try:
        uri = uri_constructor[asset_type]
    except KeyError:
        raise NotImplementedError(
            f"URI constructor for asset type {asset_type} not implemented"
        )

    return uri


def tile_uri_to_extent_geojson(uri: str) -> str:
    return uri.replace("{tile_id}.tif", "extent.geojson")


def tile_uri_to_tiles_geojson(uri: str) -> str:
    return uri.replace("{tile_id}.tif", "tiles.geojson")
