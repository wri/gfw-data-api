from typing import Any, Dict, List

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
