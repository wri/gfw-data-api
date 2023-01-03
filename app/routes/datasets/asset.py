"""Assets are replicas of the original source files.

Assets might be served in different formats, attribute values might be
altered, additional attributes added, and feature resolution might have
changed. Assets are either managed or unmanaged. Managed assets are
created by the API and users can rely on data integrity. Unmanaged
assets are only loosely linked to a dataset version and users must
cannot rely on full integrity. We can only assume that unmanaged are
based on the same version and do not know the processing history.
"""

from typing import List, Optional, Tuple, Union

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request
from fastapi.responses import ORJSONResponse

from app.settings.globals import API_URL

from ...authentication.token import is_admin
from ...crud import assets
from ...errors import RecordAlreadyExistsError
from ...models.orm.assets import Asset as ORMAsset
from ...models.pydantic.assets import (
    AssetCreateIn,
    AssetResponse,
    AssetsResponse,
    AssetType,
    PaginatedAssetsResponse,
)
from ...routes import dataset_version_dependency
from ...tasks.assets import put_asset
from ...utils.paginate import paginate_collection
from ...utils.path import get_asset_uri
from ..assets import asset_response, assets_response, paginated_assets_response
from . import (
    validate_creation_options,
    verify_asset_dependencies,
    verify_version_status,
)

router = APIRouter()


@router.get(
    "/{dataset}/{version}/assets",
    response_class=ORJSONResponse,
    tags=["Versions"],
    response_model=Union[PaginatedAssetsResponse, AssetsResponse],
)
async def get_version_assets(
    *,
    dv: Tuple[str, str] = Depends(dataset_version_dependency),
    asset_type: Optional[AssetType] = Query(None, title="Filter by Asset Type"),
    asset_uri: Optional[str] = Query(None),
    is_latest: Optional[bool] = Query(None),
    is_default: Optional[bool] = Query(None),
    request: Request,
    page_number: Optional[int] = Query(
        default=None, alias="page[number]", ge=1, description="The page number."
    ),
    page_size: Optional[int] = Query(
        default=None,
        alias="page[size]",
        ge=1,
        description="The number of assets per page. Default is `10`.",
    ),
) -> Union[PaginatedAssetsResponse, AssetsResponse]:
    """Get all assets for a given dataset version.

    Will attempt to paginate if `page[size]` or `page[number]` is
    provided. Otherwise, it will attempt to return the entire list of
    assets in the response.
    """

    dataset, version = dv

    if asset_type is not None:
        a_t: Optional[List[str]] = [asset_type]
    else:
        a_t = None

    if page_number or page_size:
        try:
            data, links, meta = await paginate_collection(
                paged_items_fn=await assets.get_filtered_assets_fn(
                    dataset, version, a_t, asset_uri, is_latest, is_default
                ),
                item_count_fn=await assets.count_filtered_assets_fn(
                    dataset, version, a_t, asset_uri, is_latest, is_default
                ),
                request_url=f"{API_URL}{request.url.path}",
                page=page_number,
                size=page_size,
            )

            return await paginated_assets_response(
                assets_orm=data, links=links, meta=meta
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    all_assets: List[ORMAsset] = await assets.get_assets_by_filter(
        dataset, version, a_t, asset_uri, is_latest, is_default
    )

    return await assets_response(all_assets)


@router.post(
    "/{dataset}/{version}/assets",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=AssetResponse,
    status_code=202,
)
async def add_new_asset(
    *,
    dv: Tuple[str, str] = Depends(dataset_version_dependency),
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

    dataset, version = dv

    input_data = request.dict(exclude_none=True, by_alias=True)

    await verify_version_status(dataset, version)

    if input_data["is_managed"]:
        await verify_asset_dependencies(dataset, version, input_data["asset_type"])

    await validate_creation_options(dataset, version, input_data)

    try:
        asset_uri = get_asset_uri(
            dataset,
            version,
            input_data["asset_type"],
            input_data.get("creation_options"),
        )
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
