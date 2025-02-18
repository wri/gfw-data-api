"""Run analysis on registered datasets."""

import uuid
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query
from fastapi.openapi.models import APIKey
from fastapi.responses import ORJSONResponse

from app.models.pydantic.datamart import TreeCoverLossByDriverIn

from ...authentication.api_keys import get_api_key
from ...models.pydantic.responses import Response

router = APIRouter()

MOCK_IDS = {}


@router.get(
    "/v0/land/tree-cover-loss-by-driver",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Land"],
)
async def tree_cover_loss_by_driver_search(
    *,
    geostore_id: UUID = Query(..., title="Geostore ID"),
    canopy_cover: int = Query(..., alias="canopy_cover", title="Canopy Cover Percent"),
    api_key: APIKey = Depends(get_api_key),
):
    """Beta endpoint, currently does no real work."""

    try:
        resource_id = MOCK_IDS[f"{geostore_id}_{canopy_cover}"]["id"]
        return {"link": f"/v0/land/tree-cover-loss-by-driver/{resource_id}"}
    except KeyError:
        return ORJSONResponse(
            status_code=404,
            content={
                "status": "status",
                "message": "Not Found",
            },
        )


@router.get(
    "/v0/land/tree-cover-loss-by-driver/{uuid}",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Land"],
)
async def tree_cover_loss_by_driver_get(
    *,
    uuid: UUID = Path(..., title="Tree cover loss by driver ID"),
    api_key: APIKey = Depends(get_api_key),
):
    """"""

    resource = None
    for mock_id in MOCK_IDS.values():
        if mock_id["id"] == uuid:
            resource = mock_id

    if resource is None:
        return ORJSONResponse(
            status_code=404,
            content={
                "status": "status",
                "message": "Not Found",
            },
        )

    if resource["retries"] < 1:
        resource["retries"] += 1
        return Response(
            headers={"Retry-After": 1},
            content={"data": {"status": "pending"}, "status": "success"},
        )
    else:
        return {
            "self": f"/v0/land/tree-cover-loss-by-driver/{resource['id']}",
            "treeCoverLossByDriver": {
                "Wildfire": 10,
                "Shifting Agriculture": 12,
                "Urbanization": 7,
            },
            "metadata": {"sources": []},
        }


@router.post(
    "/v0/land/tree-cover-loss-by-driver",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Land"],
    deprecated=True,
)
async def tree_cover_loss_by_driver_post(
    request: TreeCoverLossByDriverIn, api_key: APIKey = Depends(get_api_key)
):
    # create initial Job item as pending
    # trigger background task to create item
    # return 202 accepted
    resource_id = uuid.uuid4()
    MOCK_IDS[f"{request.geostore_id}_{request.canopy_cover}"] = {
        "id": resource_id,
        "retries": 0,
    }

    return ORJSONResponse(
        status_code=202,
        content={"link": f"/v0/land/tree-cover-loss-by-driver/{resource_id}"},
    )
