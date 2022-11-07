from typing import List, Optional, Union

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import ORJSONResponse

from app.settings.globals import API_URL

from ...crud import assets
from ...models.orm.assets import Asset as ORMAsset
from ...models.pydantic.assets import AssetsResponse, AssetType, PaginatedAssetsResponse
from ...routes import DATASET_REGEX, VERSION_REGEX
from ...utils.paginate import paginate_collection
from ..assets import assets_response, paginated_assets_response

router = APIRouter()


@router.get(
    "",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=Union[PaginatedAssetsResponse, AssetsResponse],
)
async def get_assets(
    *,
    dataset: Optional[str] = Query(None, regex=DATASET_REGEX),
    version: Optional[str] = Query(None, regex=VERSION_REGEX),
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

    if (dataset and not version) or (version and not dataset):
        raise HTTPException(status_code=400, detail="Must provide dataset and version")

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
    return await assets_response(list(all_assets))
