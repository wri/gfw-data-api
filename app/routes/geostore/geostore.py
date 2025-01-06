"""Retrieve a geometry using its md5 hash for a given dataset, user defined
geometries in the datastore."""

from uuid import UUID

from fastapi import APIRouter, HTTPException, Path
from fastapi.responses import ORJSONResponse
from httpx import Response as HTTPXResponse

from ...crud import geostore
from ...errors import BadRequestError, RecordNotFoundError
from ...models.pydantic.geostore import Geostore, GeostoreIn, GeostoreResponse
from ...utils.rw_api import get_admin_list, get_boundary_by_country_code

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
    "/admin/{country_code}",
    response_class=ORJSONResponse,
    # response_model=RWAdminListResponse,
    # status_code=200,
    # tags=["Geostore"],
)
async def rw_get_boundary_by_country_code(
    *,
    country_code: str = Path(..., title="country_code")
):
    """Get a GADM boundary by country code
    (proxies request to the RW API)"""
    # FIXME: Should we be passing on things like the API key?
    result: HTTPXResponse = await get_boundary_by_country_code(country_code)

    return result
