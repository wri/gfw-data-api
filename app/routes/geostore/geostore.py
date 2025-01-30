"""Retrieve a geometry using its md5 hash for a given dataset, user defined
geometries in the datastore."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Path, Request
from fastapi.responses import ORJSONResponse

from ...crud import geostore
from ...errors import BadRequestError, RecordNotFoundError
from ...models.pydantic.geostore import (
    Geostore,
    GeostoreIn,
    GeostoreResponse,
    RWAdminListResponse,
    RWGeostoreIn,
    RWGeostoreResponse,
)
from ...utils.rw_api import (
    create_rw_geostore,
    get_admin_list,
    get_boundary_by_country_id,
    get_boundary_by_region_id,
    get_boundary_by_subregion_id,
    get_geostore_by_land_use_and_index,
    proxy_get_geostore,
)

router = APIRouter()


@router.post(
    "/",
    response_class=ORJSONResponse,
    response_model=GeostoreResponse | RWGeostoreResponse,
    status_code=201,
    tags=["Geostore"],
)
async def add_new_geostore(
    *,
    request: GeostoreIn | RWGeostoreIn,
    x_api_key: Annotated[str | None, Header()] = None,
):
    """Add geostore feature to user area of geostore.

    If request follows RW style forward to RW, otherwise create in Data
    API
    """
    if isinstance(request, RWGeostoreIn):
        result: RWGeostoreResponse = await create_rw_geostore(request, x_api_key)
        return result
    # Otherwise, meant for GFW Data API geostore
    try:
        new_user_area: Geostore = await geostore.create_user_area(request.geometry)
        return GeostoreResponse(data=new_user_area)
    except BadRequestError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get(
    "/{geostore_id}",
    response_class=ORJSONResponse,
    response_model=GeostoreResponse | RWGeostoreResponse,
    tags=["Geostore"],
)
async def get_any_geostore(
    *,
    geostore_id: str = Path(..., title="geostore_id"),
    request: Request,
    x_api_key: Annotated[str | None, Header()] = None,
):
    """Retrieve GeoJSON representation for a given geostore ID of any dataset.

    If the provided ID is in UUID style, get from the GFW Data API.
    Otherwise, forward request to RW API.
    """
    try:
        geostore_uuid = UUID(geostore_id)
        if str(geostore_uuid) == geostore_id:
            try:
                result = await geostore.get_gfw_geostore_from_any_dataset(geostore_uuid)
                return GeostoreResponse(data=result)
            except RecordNotFoundError as e:
                raise HTTPException(status_code=404, detail=str(e))
    except (AttributeError, ValueError):
        pass
    result = await proxy_get_geostore(geostore_id, request.query_params, x_api_key)
    return result


@router.get(
    "/admin/list",
    response_class=ORJSONResponse,
    response_model=RWAdminListResponse,
    tags=["Geostore"],
    include_in_schema=False,
)
async def rw_get_admin_list(
    request: Request, x_api_key: Annotated[str | None, Header()] = None
):
    """Get all Geostore IDs, names and country codes (proxies request to the RW
    API)"""
    result: RWAdminListResponse = await get_admin_list(request.query_params, x_api_key)

    return result


@router.get(
    "/admin/{country_id}",
    response_class=ORJSONResponse,
    response_model=RWGeostoreResponse,
    tags=["Geostore"],
    include_in_schema=False,
)
async def rw_get_boundary_by_country_id(
    *,
    country_id: str = Path(..., title="country_id"),
    request: Request,
    x_api_key: Annotated[str | None, Header()] = None,
):
    """Get a GADM boundary by country ID (proxies request to the RW API)"""

    result: RWGeostoreResponse = await get_boundary_by_country_id(
        country_id, request.query_params, x_api_key
    )

    return result


@router.get(
    "/admin/{country_id}/{region_id}",
    response_class=ORJSONResponse,
    response_model=RWGeostoreResponse,
    tags=["Geostore"],
    include_in_schema=False,
)
async def rw_get_boundary_by_region_id(
    *,
    country_id: str = Path(..., title="country_id"),
    region_id: str = Path(..., title="region_id"),
    request: Request,
    x_api_key: Annotated[str | None, Header()] = None,
):
    """Get a GADM boundary by country and region IDs (proxies request to the RW
    API)"""
    result: RWGeostoreResponse = await get_boundary_by_region_id(
        country_id, region_id, request.query_params, x_api_key
    )

    return result


@router.get(
    "/admin/{country_id}/{region_id}/{subregion_id}",
    response_class=ORJSONResponse,
    response_model=RWGeostoreResponse,
    tags=["Geostore"],
    include_in_schema=False,
)
async def rw_get_boundary_by_subregion_id(
    *,
    country_id: str = Path(..., title="country_id"),
    region_id: str = Path(..., title="region_id"),
    subregion_id: str = Path(..., title="subregion_id"),
    request: Request,
    x_api_key: Annotated[str | None, Header()] = None,
):
    """Get a GADM boundary by country, region, and subregion IDs (proxies
    request to the RW API)"""

    result: RWGeostoreResponse = await get_boundary_by_subregion_id(
        country_id, region_id, subregion_id, request.query_params, x_api_key
    )

    return result


@router.get(
    "/use/{land_use_type}/{index}",
    response_class=ORJSONResponse,
    response_model=RWGeostoreResponse,
    tags=["Geostore"],
    include_in_schema=False,
)
async def rw_get_geostore_by_land_use_and_index(
    *,
    land_use_type: str = Path(..., title="land_use_type"),
    index: str = Path(..., title="index"),
    request: Request,
    x_api_key: Annotated[str | None, Header()] = None,
):
    """Get a geostore object by land use type name and id.

    Deprecated, returns out of date info, but still used by Flagship.
    Present just for completeness for now. (proxies request to the RW
    API)
    """
    result: RWGeostoreResponse = await get_geostore_by_land_use_and_index(
        land_use_type, index, request.query_params, x_api_key
    )

    return result
