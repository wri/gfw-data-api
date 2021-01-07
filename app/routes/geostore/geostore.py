"""Retrieve a geometry using its md5 hash for a given dataset, user defined
geometries in the datastore."""

from uuid import UUID

from fastapi import APIRouter, HTTPException, Path
from fastapi.responses import ORJSONResponse

from ...crud import geostore
from ...errors import BadRequestError, RecordNotFoundError
from ...models.pydantic.geostore import GeostoreHydrated, GeostoreIn, GeostoreResponse

router = APIRouter()


@router.post(
    "/",
    response_class=ORJSONResponse,
    response_model=GeostoreResponse,
    status_code=201,
)
async def add_new_geostore(
    *,
    request: GeostoreIn,
    response: ORJSONResponse,
):
    """Add geostore feature to user area of geostore."""

    input_data = request.dict(exclude_none=True, by_alias=True)

    try:
        new_user_area: GeostoreHydrated = await geostore.create_user_area(**input_data)
    except BadRequestError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return GeostoreResponse(data=new_user_area)


@router.get(
    "/{geostore_id}",
    response_class=ORJSONResponse,
    response_model=GeostoreResponse,
    tags=["Geostore"],
)
async def get_geostore_root(*, geostore_id: UUID = Path(..., title="geostore_id")):
    """Retrieve GeoJSON representation for a given geostore ID of any
    dataset."""
    try:
        result: GeostoreHydrated = await geostore.get_geostore_from_anywhere(
            geostore_id
        )
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return GeostoreResponse(data=result)
