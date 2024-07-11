from collections import defaultdict
from typing import Any, Dict, List, Sequence
from urllib.parse import urlparse

from fastapi import HTTPException

from ...crud import assets
from ...crud import versions as _versions
from ...models.enum.assets import AssetType
from ...models.orm.assets import Asset as ORMAsset
from ...models.orm.versions import Version as ORMVersion
from ...tasks.raster_tile_cache_assets import raster_tile_cache_validator
from ...tasks.raster_tile_set_assets.raster_tile_set_assets import (
    raster_tile_set_validator,
)
from ...utils.aws import get_aws_files
from ...utils.google import get_gs_files

SUPPORTED_FILE_EXTENSIONS: Sequence[str] = (
    ".csv",
    ".geojson",
    ".gpkg",
    ".ndjson",
    ".shp",
    ".tif",
    ".tsv",
    ".zip",
)


async def verify_version_status(dataset, version):
    orm_version: ORMVersion = await _versions.get_version(dataset, version)

    if orm_version.status == "pending":
        raise HTTPException(
            status_code=409,
            detail="Version status is currently `pending`. "
            "Please retry once version is in status `saved`",
        )
    elif orm_version.status == "failed":
        raise HTTPException(
            status_code=400, detail="Version status is `failed`. Cannot add any assets."
        )


async def verify_asset_dependencies(dataset, version, asset_type):
    """Verify if parent asset exists."""
    asset_dependencies = {
        AssetType.dynamic_vector_tile_cache: AssetType.geo_database_table,
        AssetType.static_vector_tile_cache: AssetType.geo_database_table,
        AssetType.raster_tile_cache: AssetType.raster_tile_set,
        AssetType.shapefile: AssetType.geo_database_table,
        AssetType.ndjson: AssetType.geo_database_table,
        AssetType.grid_1x1: AssetType.geo_database_table,
        AssetType.geopackage: AssetType.geo_database_table,
        AssetType.csv: AssetType.database_table,
        AssetType.tsv: AssetType.database_table,
        AssetType.raster_tile_set: [
            AssetType.raster_tile_set,
            AssetType.geo_database_table,
        ],
        AssetType.cog: AssetType.raster_tile_set,
    }
    try:
        parent_type = asset_dependencies[asset_type]
        if not isinstance(parent_type, list):
            parent_type = [parent_type]

        orm_assets: List[ORMAsset] = await assets.get_assets_by_filter(
            dataset, version, parent_type
        )
        exists = False
        for asset in orm_assets:
            if asset.status:
                exists = True
                break
        if not exists:
            raise HTTPException(
                status_code=400,
                detail=f"Parent asset type {asset_dependencies[asset_type]} does not exist.",
            )
    except KeyError:
        raise HTTPException(
            status_code=500,
            detail=f"Creation of asset type {asset_type} not implemented.",
        )


async def validate_creation_options(
    dataset: str, version: str, input_data: Dict[str, Any]
) -> None:
    validator = {
        AssetType.raster_tile_cache: raster_tile_cache_validator,
        AssetType.raster_tile_set: raster_tile_set_validator,
    }
    try:
        await validator[input_data["asset_type"]](dataset, version, input_data)
    except KeyError:
        pass


# I cannot seem to satisfy mypy WRT the type of this default dict. Last thing I tried:
# DefaultDict[str, Callable[[str, str, int, int, ...], List[str]]]
source_uri_lister_constructor = defaultdict((lambda: lambda w, x, limit=None, exit_after_max=None, extensions=None: list()))  # type: ignore
source_uri_lister_constructor.update(**{"gs": get_gs_files, "s3": get_aws_files})  # type: ignore


def _verify_source_file_access(sources: List[str]) -> None:

    # TODO:
    # 1. Making the list functions asynchronous and using asyncio.gather
    # to check for valid sources in a non-blocking fashion would be good.
    # Perhaps use the aioboto3 package for aws, gcloud-aio-storage for gcs.
    # 2. It would be nice if the acceptable file extensions were passed
    # into this function so we could say, for example, that there must be
    # TIFFs found for a new raster tile set, but a CSV is required for a new
    # vector tile set version. Even better would be to specify whether
    # paths to individual files or "folders" (prefixes) are allowed.

    invalid_sources: List[str] = list()

    for source in sources:
        url_parts = urlparse(source, allow_fragments=False)
        list_func = source_uri_lister_constructor[url_parts.scheme.lower()]
        bucket = url_parts.netloc
        prefix = url_parts.path.lstrip("/")

        # Allow pseudo-globbing: Tolerate a "*" at the end of a
        # src_uri entry to allow partial prefixes (for example
        # /bucket/prefix_part_1/prefix_fragment* will match
        # /bucket/prefix_part_1/prefix_fragment_1.tif and
        # /bucket/prefix_part_1/prefix_fragment_2.tif, etc.)
        # If the prefix doesn't end in "*" or an acceptable file extension
        # add a "/" to the end of the prefix to enforce it being a "folder".
        new_prefix: str = prefix
        if new_prefix.endswith("*"):
            new_prefix = new_prefix[:-1]
        elif not new_prefix.endswith("/") and not any(
            [new_prefix.endswith(suffix) for suffix in SUPPORTED_FILE_EXTENSIONS]
        ):
            new_prefix += "/"

        if not list_func(
            bucket,
            new_prefix,
            limit=10,
            exit_after_max=1,
            extensions=SUPPORTED_FILE_EXTENSIONS,
        ):
            invalid_sources.append(source)

    if invalid_sources:
        raise HTTPException(
            status_code=400,
            detail=(
                "Cannot access all of the source files (non-existent or access denied). "
                f"Invalid sources: {invalid_sources}"
            ),
        )
