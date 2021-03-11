from typing import Tuple
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.responses import ORJSONResponse

from ...crud import geostore
from ...errors import RecordNotFoundError
from ...models.pydantic.geostore import Geostore, GeostoreResponse
from ...routes import dataset_version_dependency

router = APIRouter()


@router.get(
    "/{dataset}/{version}/geostore/{geostore_id}",
    response_class=ORJSONResponse,
    response_model=GeostoreResponse,
    tags=["Geostore"],
)
async def get_geostore_by_version(
    *,
    dv: Tuple[str, str] = Depends(dataset_version_dependency),
    geostore_id: UUID = Path(..., title="geostore_id"),
):
    """Retrieve GeoJSON representation for a given geostore ID of a dataset
    version.

    Obtain geostore ID from feature attributes.
    """
    dataset, version = dv
    try:
        result: Geostore = await geostore.get_geostore_by_version(
            dataset, version, geostore_id
        )
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return GeostoreResponse(data=result)
