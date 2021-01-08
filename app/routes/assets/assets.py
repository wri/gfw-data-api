from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import ORJSONResponse

from ...crud import assets
from ...models.orm.assets import Asset as ORMAsset
from ...models.pydantic.assets import AssetsResponse, AssetType
from ...routes import DATASET_REGEX, VERSION_REGEX
from ..assets import assets_response

router = APIRouter()


@router.get(
    "/",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=AssetsResponse,
)
async def get_assets(
    *,
    dataset: Optional[str] = Query(None, regex=DATASET_REGEX),
    version: Optional[str] = Query(None, regex=VERSION_REGEX),
    asset_type: Optional[AssetType] = Query(None, title="Filter by Asset Type"),
    asset_uri: Optional[str] = Query(None),
    is_latest: Optional[bool] = Query(None),
    is_default: Optional[bool] = Query(None)
):
    """Get all assets for a given dataset version."""

    if (dataset and not version) or (version and not dataset):
        raise HTTPException(status_code=400, detail="Must provide dataset and version")

    if asset_type is not None:
        a_t: Optional[List[str]] = [asset_type]
    else:
        a_t = None

    data: List[ORMAsset] = await assets.get_assets_by_filter(
        dataset, version, a_t, asset_uri, is_latest, is_default
    )

    return await assets_response(list(data))
