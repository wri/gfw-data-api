"""Assets are replicas of the original source files.

Assets might be served in different formats, attribute values might be
altered, additional attributes added, and feature resolution might have
changed. Assets are either managed or unmanaged. Managed assets are
created by the API and users can rely on data integrity. Unmanaged
assets are only loosely linked to a dataset version and users must
cannot rely on full integrity. We can only assume that unmanaged are
based on the same version and do not know the processing history.
"""


from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from fastapi.responses import ORJSONResponse

from ...crud import assets, versions
from ...errors import RecordAlreadyExistsError, RecordNotFoundError
from ...models.orm.assets import Asset as ORMAsset
from ...models.pydantic.assets import (
    AssetCreateIn,
    AssetResponse,
    AssetsResponse,
    AssetType,
)
from ...routes import dataset_dependency, is_admin, version_dependency
from ...tasks.assets import put_asset
from ...utils.path import get_asset_uri
from ..assets import asset_response, assets_response
from . import verify_version_status

router = APIRouter()


@router.get(
    "/{dataset}/{version}/assets",
    response_class=ORJSONResponse,
    tags=["Versions"],
    response_model=AssetsResponse,
)
async def get_version_assets(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    asset_type: Optional[AssetType] = Query(None, title="Filter by Asset Type"),
    asset_uri: Optional[str] = Query(None),
    is_latest: Optional[bool] = Query(None),
    is_default: Optional[bool] = Query(None),
):
    """Get all assets for a given dataset version."""

    try:
        await versions.get_version(dataset, version)
    except RecordNotFoundError as e:
        raise HTTPException(status_code=400, detail=str(e))

    data: List[ORMAsset] = await assets.get_assets_by_filter(
        dataset, version, asset_type, asset_uri, is_latest, is_default
    )

    return await assets_response(data)


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

    input_data = request.dict(exclude_none=True, by_alias=True)

    await verify_version_status(dataset, version)

    try:
        asset_uri = get_asset_uri(dataset, version, input_data["asset_type"])
    except NotImplementedError:
        raise HTTPException(
            status_code=501,
            detail=f"Procedure for creating asset type {input_data['asset_type']} not implemented",
        )

    input_data["asset_uri"] = asset_uri

    try:
        row: ORMAsset = await assets.create_asset(dataset, version, **input_data)
    except RecordAlreadyExistsError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except NotImplementedError as e:
        raise HTTPException(status_code=501, detail=str(e))

    background_tasks.add_task(
        put_asset, row.asset_type, row.asset_id, dataset, version, input_data
    )
    response.headers["Location"] = f"/{dataset}/{version}/asset/{row.asset_id}"
    return await asset_response(row)
