"""Retrieve a geometry using its md5 hash for a given dataset, user defined
geometries in the datastore."""
from typing import Dict
from uuid import UUID

from fastapi import APIRouter, HTTPException, Path
from fastapi.responses import ORJSONResponse
from httpx import Response as HTTPXResponse

from ...crud import geostore
from ...errors import BadRequestError, RecordNotFoundError
from ...models.pydantic.geostore import Geostore, GeostoreIn, GeostoreResponse, RWFindByIDsIn
from ...utils.rw_api import (
    find_by_ids,
    get_admin_list,
    get_boundary_by_country_id,
    get_boundary_by_region_id,
    get_boundary_by_subregion_id,
    get_geostore_by_land_use_and_index,
    get_geostore_by_wdpa_id,
    get_view_geostore_by_id,
)

router = APIRouter()


@router.post(
    "/",
    response_class=ORJSONResponse,
    response_model=GeostoreResponse,
    status_code=201,
    tags=["Geostore"],
)
async def add_new_geostore(
    *,
    request: GeostoreIn,
    response: ORJSONResponse,  # Is this used?
):
    """Add geostore feature to user area of geostore."""

    try:
        new_user_area: Geostore = await geostore.create_user_area(request.geometry)
    except BadRequestError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return GeostoreResponse(data=new_user_area)


# Endpoint proxied to RW geostore microservice:
@router.get(
    "/{rw_geostore_id}/view",
    response_class=ORJSONResponse,
    # response_model=GeostoreResponse,
    # tags=["Geostore"],
)
async def rw_get_view_geostore_by_id(*, rw_geostore_id: str = Path(..., title="rw_geostore_id")):
    """Get a geostore object by Geostore id and view at GeoJSON.io
    (proxies request to the RW API)"""
    # FIXME: Should we be passing on things like the API key?
    result: HTTPXResponse = await get_view_geostore_by_id(rw_geostore_id)

    return result


@router.get(
    "/{geostore_id}",
    response_class=ORJSONResponse,
    response_model=GeostoreResponse,
    tags=["Geostore"],
)
async def get_any_geostore(*, geostore_id: UUID = Path(..., title="geostore_id")):
    """Retrieve GeoJSON representation for a given geostore ID of any
    dataset."""
    try:
        result: Geostore = await geostore.get_gfw_geostore_from_any_dataset(geostore_id)
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return GeostoreResponse(data=result)


# Endpoints proxied to RW geostore microservice:

@router.get(
    "/admin/list",
    response_class=ORJSONResponse,
    # response_model=RWAdminListResponse,
    # status_code=200,
    # tags=["Geostore"],
)
async def rw_get_admin_list():
    """Get all Geostore IDs, names and country codes
    (proxies request to the RW API)"""
    # FIXME: Should we be passing on things like the API key?
    result: HTTPXResponse = await get_admin_list()

    return result


@router.get(
    "/admin/{country_id}",
    response_class=ORJSONResponse,
    # response_model=RWAdminListResponse,
    # status_code=200,
    # tags=["Geostore"],
)
async def rw_get_boundary_by_country_id(
    *,
    country_id: str = Path(..., title="country_id")
):
    """Get a GADM boundary by country ID
    (proxies request to the RW API)"""
    # FIXME: Should we be passing on things like the API key?
    result: HTTPXResponse = await get_boundary_by_country_id(country_id)

    return result


@router.get(
    "/admin/{country_id}/{region_id}",
    response_class=ORJSONResponse,
    # response_model=RWAdminListResponse,
    # status_code=200,
    # tags=["Geostore"],
)
async def rw_get_boundary_by_region_id(
    *,
    country_id: str = Path(..., title="country_id"),
    region_id: str = Path(..., title="region_id")
):
    """Get a GADM boundary by country and region IDs
    (proxies request to the RW API)"""
    # FIXME: Should we be passing on things like the API key?
    result: HTTPXResponse = await get_boundary_by_region_id(country_id, region_id)

    return result


@router.get(
    "/admin/{country_id}/{region_id}/{subregion_id}",
    response_class=ORJSONResponse,
    # response_model=RWAdminListResponse,
    # status_code=200,
    # tags=["Geostore"],
)
async def rw_get_boundary_by_subregion_id(
    *,
    country_id: str = Path(..., title="country_id"),
    region_id: str = Path(..., title="region_id"),
    subregion_id: str = Path(..., title="subregion_id")
):
    """Get a GADM boundary by country, region, and subregion IDs
    (proxies request to the RW API)"""
    # FIXME: Should we be passing on things like the API key?
    result: HTTPXResponse = await get_boundary_by_subregion_id(country_id, region_id, subregion_id)

    return result


@router.post(
    "/find_by_ids",
    response_class=ORJSONResponse,
    # response_model=RWAdminListResponse,
    # status_code=200,
    # tags=["Geostore"],
)
async def rw_find_by_ids(
    request: RWFindByIDsIn,
):
    """Get one or more geostore objects by IDs
    (proxies request to the RW API)"""
    # FIXME: Should we be passing on things like the API key?
    payload: Dict = request.dict(exclude_none=True, by_alias=True)

    result: HTTPXResponse = await find_by_ids(payload)

    return result


@router.get(
    "/use/{land_use_type}/{index}",
    response_class=ORJSONResponse,
    # response_model=RWAdminListResponse,
    # status_code=200,
    # tags=["Geostore"],
)
async def rw_get_geostore_by_land_use_and_index(
    *,
    land_use_type: str = Path(..., title="land_use_type"),
    index: str = Path(..., title="index")
):
    """Get a geostore object by land use type name and id
    (proxies request to the RW API)"""
    # FIXME: Should we be passing on things like the API key?
    result: HTTPXResponse = await get_geostore_by_land_use_and_index(land_use_type, index)

    return result


@router.get(
    "/wdpa/{wdpa_id}",
    response_class=ORJSONResponse,
    # response_model=RWAdminListResponse,
    # status_code=200,
    # tags=["Geostore"],
)
async def rw_get_geostore_by_wdpa_id(
    *,
    wdpa_id: str = Path(..., title="wdpa_id")
):
    """Get a geostore object by WDPA ID
    (proxies request to the RW API)"""
    # FIXME: Should we be passing on things like the API key?
    result: HTTPXResponse = await get_geostore_by_wdpa_id(wdpa_id)

    return result
