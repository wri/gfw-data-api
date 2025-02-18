"""Run analysis on registered datasets."""

import random
import uuid
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, Request
from fastapi.logger import logger
from fastapi.openapi.models import APIKey
from fastapi.responses import ORJSONResponse

from app.models.pydantic.datamart import TreeCoverLossByDriverIn

from ...authentication.api_keys import get_api_key
from ...models.pydantic.responses import Response

router = APIRouter()


@router.get(
    "/tree-cover-loss-by-driver",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Land"],
)
async def tree_cover_loss_by_driver_search(
    *,
    geostore_id: UUID = Query(..., title="Geostore ID"),
    canopy_cover: int = Query(..., alias="canopy_cover", title="Canopy Cover Percent"),
    request: Request,
    api_key: APIKey = Depends(get_api_key),
):
    """Search if a resource exists for a given geostore and canopy cover."""
    # create mock_ids state if it doesn't exist
    if not hasattr(request.app.state, "mock_ids"):
        request.app.state.mock_ids = {}

    try:
        resource_id = request.app.state.mock_ids[f"{geostore_id}_{canopy_cover}"]["id"]
        return ORJSONResponse(
            status_code=200,
            content={
                "status": "success",
                "data": {"link": f"/v0/land/tree-cover-loss-by-driver/{resource_id}"},
            },
        )
        return
    except KeyError:
        return ORJSONResponse(
            status_code=404,
            content={
                "status": "failed",
                "message": "Not Found",
            },
        )


@router.get(
    "/tree-cover-loss-by-driver/{uuid}",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Land"],
)
async def tree_cover_loss_by_driver_get(
    *,
    uuid: UUID = Path(..., title="Tree cover loss by driver ID"),
    request: Request,
    api_key: APIKey = Depends(get_api_key),
):
    """Retrieve a tree cover loss by drivers resource."""
    # create mock_ids state if it doesn't exist
    if not hasattr(request.app.state, "mock_ids"):
        request.app.state.mock_ids = {}

    logger.info(request.app.state.mock_ids)

    resource = None
    for mock_id in request.app.state.mock_ids.values():
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

    if resource["retries"] < 3:
        resource["retries"] += 1
        return ORJSONResponse(
            status_code=200,
            headers={"Retry-After": "1"},
            content={"data": {"status": "pending"}, "status": "success"},
        )
    else:
        return ORJSONResponse(
            status_code=200,
            content={
                "data": {
                    "self": f"/v0/land/tree-cover-loss-by-driver/{resource['id']}",
                    "treeCoverLossByDriver": {
                        "Permanent agriculture": 10,
                        "Hard commodities": 12,
                        "Shifting cultivation": 7,
                        "Forest management": 93.4,
                        "Wildfires": 42,
                        "Settlements and infrastructure": 13.562,
                        "Other natural disturbances": 6,
                    },
                    "metadata": {
                        "sources": [
                            {"dataset": "umd_tree_cover_loss", "version": "v1.11"},
                            {
                                "dataset": "wri_google_tree_cover_loss_by_drivers",
                                "version": "v1.11",
                            },
                            {
                                "dataset": "umd_tree_cover_density_2000",
                                "version": "v1.11",
                            },
                        ]
                    },
                },
                "status": "success",
            },
        )


@router.post(
    "/tree-cover-loss-by-driver",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Land"],
    deprecated=True,
)
async def tree_cover_loss_by_driver_post(
    data: TreeCoverLossByDriverIn,
    request: Request,
    api_key: APIKey = Depends(get_api_key),
):
    """Create new tree cover loss by drivers resource for a given geostore and
    canopy cover."""

    # create initial Job item as pending
    # trigger background task to create item
    # return 202 accepted
    resource_id = uuid.uuid4()

    # create mock_ids state if it doesn't exist
    if not hasattr(request.app.state, "mock_ids"):
        request.app.state.mock_ids = {}

    # mocks randomness of analysis time
    retries = random.randint(0, 3)
    request.app.state.mock_ids[f"{data.geostore_id}_{data.canopy_cover}"] = {
        "id": resource_id,
        "retries": retries,
    }

    return ORJSONResponse(
        status_code=202,
        content={
            "data": {
                "link": f"/v0/land/tree-cover-loss-by-driver/{resource_id}",
            },
            "status": "success",
        },
    )
