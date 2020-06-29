"""Assets are replicas of the original source files.

Assets might be served in different formats, attribute values might be
altered, additional attributes added, and feature resolution might have
changed. Assets are either managed or unmanaged. Managed assets are
created by the API and users can rely on data integrity. Unmanaged
assets are only loosly linked to a dataset version and users must cannot
rely on full integrety. We can only assume that unmanaged are based on
the same version and do not know the processing history.
"""

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, Path, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import ORJSONResponse

from ...crud import assets, versions
from ...errors import ClientError
from ...models.orm.assets import Asset as ORMAsset
from ...models.orm.versions import Version as ORMVersion
from ...models.pydantic.assets import (
    Asset,
    AssetCreateIn,
    AssetResponse,
    AssetsResponse,
    AssetType,
    AssetUpdateIn,
)
from ...routes import dataset_dependency, is_admin, version_dependency
from ...tasks.assets import create_asset
from ...tasks.delete_assets import (
    delete_database_table,
    delete_dynamic_vector_tile_cache_assets,
    delete_raster_tileset_assets,
    delete_static_raster_tile_cache_assets,
    delete_static_vector_tile_cache_assets,
)
from . import verify_version_status

router = APIRouter()

# TODO:
#  - Assets should have config parameters to allow specifying creation options


@router.get(
    "/{dataset}/{version}/assets",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=AssetsResponse,
)
async def get_assets(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    asset_type: Optional[AssetType] = Query(None, title="Filter by Asset Type"),
):
    """Get all assets for a given dataset version."""

    rows: List[ORMAsset] = await assets.get_assets(dataset, version)

    # Filter rows by asset type
    data = list()
    if asset_type:
        for row in rows:
            if row.asset_type == asset_type:
                data.append(row)
    else:
        data = rows

    return AssetsResponse(data=data)


@router.get(
    "/{dataset}/{version}/assets/{asset_id}",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=AssetResponse,
)
async def get_asset(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    asset_id: UUID = Path(...),
) -> AssetResponse:
    """Get a specific asset."""
    row: ORMAsset = await assets.get_asset(asset_id)

    if row.dataset != dataset and row.version != version:
        raise ClientError(
            status_code=404,
            detail=f"Could not find requested asset {dataset}/{version}/{asset_id}",
        )

    return await _asset_response(row)


@router.post(
    "/{dataset}/{version}/assets",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=AssetResponse,
    status_code=202,
)
async def add_new_asset(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    request: AssetCreateIn,
    background_tasks: BackgroundTasks,
    is_authorized: bool = Depends(is_admin),
    response: ORJSONResponse,
) -> AssetResponse:
    """Add a new asset to a dataset version. Managed assets will be generated
    by the API itself. In that case, the Asset URI is read only and will be set
    automatically.

    If the asset is not managed, you need to specify an Asset URI to
    link to.
    """
    input_data = request.dict()

    await verify_version_status(dataset, version)

    row: ORMAsset = await assets.create_asset(dataset, version, **input_data)
    background_tasks.add_task(
        create_asset, row.asset_type, row.asset_id, dataset, version, input_data
    )
    response.headers["Location"] = f"/{dataset}/{version}/asset/{row.asset_id}"
    return await _asset_response(row)


@router.patch(
    "/{dataset}/{version}/assets/{asset_id}",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=AssetResponse,
)
async def update_asset(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    asset_id: UUID = Path(...),
    request: AssetUpdateIn,
    is_authorized: bool = Depends(is_admin),
    response: ORJSONResponse,
) -> AssetResponse:
    """Update Asset metadata."""

    input_data = request.dict(exclude_unset=True)
    await verify_version_status(dataset, version)

    row: ORMAsset = await assets.update_asset(asset_id, **input_data)
    return await _asset_response(row)


@router.delete(
    "/{dataset}/{version}/assets/{asset_id}",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=AssetResponse,
)
async def delete_asset(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    asset_id: UUID = Path(...),
    is_authorized: bool = Depends(is_admin),
    background_tasks: BackgroundTasks,
) -> AssetResponse:
    """Delete selected asset.

    For managed assets, all resources will be deleted. For non-managed
    assets, only the link will be deleted.
    """

    row: ORMAsset = await assets.get_asset(asset_id)

    if row.is_default:
        raise ClientError(
            status_code=409,
            detail="Deletion failed. You cannot delete a default asset. "
            "To delete a default asset you must delete the parent version.",
        )

    if row.asset_type == AssetType.dynamic_vector_tile_cache:
        background_tasks.add_task(
            delete_dynamic_vector_tile_cache_assets,
            dataset,
            version,
            row.creation_options.implementation,
        )

    elif row.asset_type == AssetType.static_vector_tile_cache:
        background_tasks.add_task(
            delete_static_vector_tile_cache_assets,
            dataset,
            version,
            row.creation_options.implementation,
        )

    elif row.asset_type == AssetType.static_raster_tile_cache:
        background_tasks.add_task(
            delete_static_raster_tile_cache_assets,
            dataset,
            version,
            row.creation_options.implementation,
        )

    elif row.asset_type == AssetType.raster_tile_set:
        background_tasks.add_task(
            delete_raster_tileset_assets,
            dataset,
            version,
            row.creation_options.srid,
            row.creation_options.size,
            row.creation_options.col,
            row.creation_options.value,
        )
    elif row.asset_type == AssetType.database_table:
        background_tasks.add_task(delete_database_table, dataset, version)
    else:
        raise ClientError(
            status_code=400,
            detail=f"Cannot delete asset of type {row.asset_type}. Not implemented.",
        )

    row = await assets.delete_asset(asset_id)

    return await _asset_response(row)


async def _asset_response(asset_orm: ORMAsset) -> AssetResponse:
    """Serialize ORM response."""
    data = Asset.from_orm(asset_orm)  # .dict(by_alias=True)
    return AssetResponse(data=data)


async def _assets_response(assets_orm: List[ORMAsset]) -> AssetsResponse:
    """Serialize ORM response."""
    data = [Asset.from_orm(asset) for asset in assets_orm]  # .dict(by_alias=True)
    return AssetsResponse(data=data)
