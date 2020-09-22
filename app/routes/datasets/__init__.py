from typing import List

from fastapi import HTTPException

from ...crud import assets
from ...crud import versions as _versions
from ...models.enum.assets import AssetType
from ...models.orm.assets import Asset as ORMAsset
from ...models.orm.versions import Version as ORMVersion


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
        AssetType.static_raster_tile_cache: AssetType.raster_tile_set,
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
        orm_assets: List[ORMAsset] = await assets.get_assets_by_filter(
            dataset, version, asset_dependencies[asset_type]
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
