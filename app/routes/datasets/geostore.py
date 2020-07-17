from uuid import UUID

from fastapi import APIRouter, Depends, Path
from fastapi.responses import ORJSONResponse

from ...crud import geostore
from ...models.pydantic.geostore import GeostoreHydrated, GeostoreResponse
from ...routes import dataset_dependency, version_dependency

router = APIRouter()


@router.get(
    "/{dataset}/{version}/geostore/{geostore_id}",
    response_class=ORJSONResponse,
    response_model=GeostoreResponse,
    tags=["Geostore"],
)
async def get_geostore_by_version(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    geostore_id: UUID = Path(..., title="geostore_id"),
):
    """Retrieve GeoJSON representation for a given geostore ID of a dataset
    version.

    Obtain geostore ID from feature attributes.
    """
    result: GeostoreHydrated = await geostore.get_geostore_by_version(
        dataset, version, geostore_id
    )
    return GeostoreResponse(data=result)
