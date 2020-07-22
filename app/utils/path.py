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


def is_zipped(s3_uri: str) -> bool:
    """Get basename of source file.

    If Zipfile, add VSIZIP prefix for GDAL
    """
    bucket, key = split_s3_path(s3_uri)
    client = get_s3_client()
    _, ext = os.path.splitext(s3_uri)

    try:
        header = client.head_object(Bucket=bucket, Key=key)
        # TODO: moto does not return the correct ContenType so have to go for the ext
        if header["ContentType"] == "application/x-zip-compressed" or ext == ".zip":
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
) -> str:

    if not creation_options:
        creation_options = {}
    srid = creation_options.get("srid", None)
    size = creation_options.get("size", None)
    col = creation_options.get("col", None)
    value = creation_options.get("value", None)

    uri_constructor: Dict[str, str] = {
        AssetType.dynamic_vector_tile_cache: f"{TILE_CACHE_URL}/{dataset}/{version}/dynamic/{{z}}/{{x}}/{{y}}.pbf",
        AssetType.static_vector_tile_cache: f"{TILE_CACHE_URL}/{dataset}/{version}/default/{{z}}/{{x}}/{{y}}.pbf",
        AssetType.static_raster_tile_cache: f"{TILE_CACHE_URL}/{dataset}/{version}/default/{{z}}/{{x}}/{{y}}.png",
        AssetType.shapefile: f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/vector/epsg:4326/{dataset}_{version}.shp.zip",
        AssetType.ndjson: f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/vector/epsg:4326/{dataset}_{version}.ndjson",
        AssetType.geopackage: f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/vector/epsg:4326/{dataset}_{version}{dataset}.gpkg",
        AssetType.csv: f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/text/{dataset}_{version}.csv",
        AssetType.tsv: f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/text/{dataset}_{version}.tsv",
        AssetType.geo_database_table: f"{API_URL}/dataset/{dataset}/{version}/query",
        AssetType.database_table: f"{API_URL}/dataset/{dataset}/{version}/query",
        AssetType.raster_tile_set: f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/raster/{srid}/{size}/{col}/{value}/geotiff/{{tile_id}}.tif",
    }

    try:
        uri = uri_constructor[asset_type]
    except KeyError:
        raise NotImplementedError(
            f"URI constructor for asset type {asset_type} not implemented"
        )

    return uri
