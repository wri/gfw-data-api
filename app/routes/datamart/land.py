"""Run analysis on registered datasets."""

import json
import os
import uuid
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, Path, Query
from fastapi.openapi.models import APIKey
from fastapi.responses import ORJSONResponse

from app.models.pydantic.datamart import TreeCoverLossByDriverIn
from app.settings.globals import API_URL
from app.tasks.datamart.land import compute_tree_cover_loss_by_driver

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
    canopy_cover: int = Query(30, alias="canopy_cover", title="Canopy Cover Percent"),
    api_key: APIKey = Depends(get_api_key),
):
    """Search if a resource exists for a given geostore and canopy cover."""

    resource_id = _get_resource_id(geostore_id, canopy_cover)

    if os.path.exists(f"/tmp/{resource_id}"):
        return ORJSONResponse(
            status_code=200,
            content={
                "status": "success",
                "data": {
                    "link": f"{API_URL}/v0/land/tree-cover-loss-by-driver/{resource_id}"
                },
            },
        )

    return ORJSONResponse(
        status_code=404,
        content={
            "status": "failed",
            "message": "Not Found",
        },
    )


@router.get(
    "/tree-cover-loss-by-driver/{resource_id}",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Land"],
)
async def tree_cover_loss_by_driver_get(
    *,
    resource_id: UUID = Path(..., title="Tree cover loss by driver ID"),
    api_key: APIKey = Depends(get_api_key),
):
    """Retrieve a tree cover loss by drivers resource."""
    try:
        with open(f"/tmp/{resource_id}", "r") as f:
            result = json.loads(f.read())

        headers = {}
        if result["status"] == "pending":
            headers = {"Retry-After": "1"}

        return ORJSONResponse(
            status_code=200,
            headers=headers,
            content={"data": result, "status": "success"},
        )
    except FileNotFoundError:
        return ORJSONResponse(
            status_code=404,
            content={
                "status": "failed",
                "message": "Not Found",
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
    background_tasks: BackgroundTasks,
    api_key: APIKey = Depends(get_api_key),
):
    """Create new tree cover loss by drivers resource for a given geostore and
    canopy cover."""

    # create initial Job item as pending
    # trigger background task to create item
    # return 202 accepted
    resource_id = _get_resource_id(data.geostore_id, data.canopy_cover)
    pending_result = {"data": {"status": "pending"}, "status": "success"}

    with open(f"/tmp/{resource_id}", "w") as f:
        f.write(json.dumps(pending_result))

    background_tasks.add_task(
        compute_tree_cover_loss_by_driver,
        resource_id,
        data.geostore_id,
        data.canopy_cover,
    )

    return ORJSONResponse(
        status_code=202,
        content={
            "data": {
                "link": f"{API_URL}/v0/land/tree-cover-loss-by-driver/{resource_id}",
            },
            "status": "success",
        },
    )


def _get_resource_id(geostore_id, canopy_cover):
    return uuid.uuid5(uuid.NAMESPACE_OID, f"{geostore_id}_{canopy_cover}")
