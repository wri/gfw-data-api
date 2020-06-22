"""
Assets are replicas of the original source files. Assets might
be served in different formats, attribute values might be altered, additional attributes added,
and feature resolution might have changed. Assets are either managed or unmanaged. Managed assets
are created by the API and users can rely on data integrity. Unmanaged assets are only loosly linked
to a dataset version and users must cannot rely on full integrety. We can only assume that unmanaged
are based on the same version and do not know the processing history."""

from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Path, Query
from fastapi.responses import ORJSONResponse

from ...crud import assets, versions
from ...models.orm.assets import Asset as ORMAsset
from ...models.orm.versions import Version as ORMVersion
from ...models.pydantic.assets import (
    Asset,
    AssetCreateIn,
    AssetResponse,
    AssetsResponse,
    AssetType,
)
from ...routes import dataset_dependency, is_admin, version_dependency
from ...tasks.assets import create_asset

router = APIRouter()


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
        raise HTTPException(
            status_code=404,
            detail=f"Could not find requested asset {dataset}/{version}/{asset_id}",
        )

    return await _asset_response(row)


@router.get(
    "/assets",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=AssetsResponse,
)
async def get_assets_root(
    *, asset_type: Optional[AssetType] = Query(None, title="Filter by Asset Type")
) -> AssetsResponse:
    """Get all assets."""
    if asset_type:
        rows: List[ORMAsset] = await assets.get_assets_by_type(asset_type)
    else:
        rows = await assets.get_all_assets()

    return await _assets_response(rows)


@router.get(
    "assets/{asset_id}",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=AssetResponse,
)
async def get_asset_root(*, asset_id: UUID = Path(...)) -> AssetResponse:
    """Get a specific asset."""
    row: ORMAsset = await assets.get_asset(asset_id)
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
    """

    Add a new asset to a dataset version. Managed assets will be generated by the API itself.
    In that case, the Asset URI is read only and will be set automatically.

    If the asset is not managed, you need to specify an Asset URI to link to.

    """
    input_data = request.dict()

    orm_version: ORMVersion = await versions.get_version(dataset, version)

    if orm_version.status == "pending":
        raise HTTPException(
            status_code=409,
            detail="Version status is currently `pending`."
            "Please retry once version is in status `saved`",
        )
    elif orm_version.status == "failed":
        raise HTTPException(
            status_code=400, detail="Version status is `failed`. Cannot add any assets."
        )
    else:
        row: ORMAsset = await assets.create_asset(dataset, version, **input_data)
        background_tasks.add_task(
            create_asset, row.asset_id, dataset, version, input_data
        )
        response.headers["Location"] = f"/{dataset}/{version}/asset/{row.asset_id}"
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
) -> AssetResponse:
    """

    Delete selected asset.
    For managed assets, all resources will be deleted. For non-managed assets, only the link will be deleted.
    """
    raise NotImplementedError


async def _asset_response(asset_orm: ORMAsset) -> AssetResponse:
    """

    Serialize ORM response.
    """
    data = Asset.from_orm(asset_orm)  # .dict(by_alias=True)
    return AssetResponse(data=data)


async def _assets_response(assets_orm: List[ORMAsset]) -> AssetsResponse:
    """

    Serialize ORM response.
    """
    data = [Asset.from_orm(asset) for asset in assets_orm]  # .dict(by_alias=True)
    return AssetsResponse(data=data)
