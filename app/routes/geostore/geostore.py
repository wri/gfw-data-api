"""Retrieve a geometry using its md5 hash for a given dataset, user defined
geometries in the datastore."""

from uuid import UUID

from fastapi import APIRouter, Depends, Path
from fastapi.responses import ORJSONResponse

from ...application import db
from ...crud import geostore
from ...models.orm.geostore import Geostore as ORMGeostore
from ...models.pydantic.geostore import Geostore, GeostoreIn, GeostoreResponse
from ...routes import dataset_dependency, version_dependency

router = APIRouter()


@router.post(
    "/geostore", response_class=ORJSONResponse,
)
async def add_new_geostore(
    *, request: GeostoreIn, response: ORJSONResponse,
):
    """Add geostore feature to User geostore."""

    input_data = request.dict(exclude_none=True, by_alias=True)

    new_user_area = await geostore.create_user_area(**input_data)

    return GeostoreResponse(data=new_user_area)


@router.get(
    "/geostore/{geostore_id}", response_class=ORJSONResponse, tags=["Geostore"],
)
async def get_geostore_root(*, geostore_id: UUID = Path(..., title="geostore_id")):
    """Retrieve GeoJSON representation for a given geostore ID of any
    dataset."""
    result = await geostore.get_user_area_geostore(geostore_id)
    return GeostoreResponse(data=result)


@router.get(
    "/{dataset}/{version}/geostore/{geostore_id}",
    response_class=ORJSONResponse,
    tags=["Geostore"],
)
async def get_geostore(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    geostore_id: UUID = Path(..., title="geostore_id"),
):
    """Retrieve GeoJSON representation for a given geostore ID of a dataset
    version.

    Obtain geostore ID from feature attributes.
    """
    return await geostore.get_particular_geostore(dataset, version, geostore_id)
