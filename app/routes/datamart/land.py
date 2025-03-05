"""Run analysis on registered datasets."""

import uuid
from typing import Optional
from uuid import UUID

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    HTTPException,
    Path,
    Query,
    Request,
    Response,
)
from fastapi.openapi.models import APIKey
from fastapi.responses import ORJSONResponse

from app.crud import datamart as datamart_crud
from app.errors import RecordNotFoundError
from app.models.enum.geostore import GeostoreOrigin
from app.models.pydantic.datamart import AnalysisStatus
from app.models.pydantic.datamart import (
    DataMartResource,
    DataMartResourceLink,
    DataMartResourceLinkResponse,
    TreeCoverLossByDriver,
    TreeCoverLossByDriverIn,
    TreeCoverLossByDriverResponse,
)
from app.settings.globals import API_URL
from app.tasks.datamart.land import (
    DEFAULT_LAND_DATASET_VERSIONS,
    compute_tree_cover_loss_by_driver,
)
from app.utils.geostore import get_geostore

from ...authentication.api_keys import get_api_key

router = APIRouter()


@router.get(
    "/tree-cover-loss-by-driver",
    response_class=ORJSONResponse,
    response_model=DataMartResourceLinkResponse,
    tags=["Land"],
    status_code=200,
)
async def tree_cover_loss_by_driver_search(
    *,
    geostore_id: UUID = Query(..., title="Geostore ID"),
    canopy_cover: int = Query(30, alias="canopy_cover", title="Canopy cover percent"),
    dataset: Optional[list[str]] = Query([], title="Dataset overrides"),
    version: Optional[list[str]] = Query([], title="Version overrides"),
    api_key: APIKey = Depends(get_api_key),
):
    """Search if a resource exists for a given geostore and canopy cover."""
    # Merge dataset version overrides with default dataset versions
    query_dataset_version = {ds: v for ds, v in zip(dataset, version)}
    dataset_version = DEFAULT_LAND_DATASET_VERSIONS | query_dataset_version

    resource_id = _get_resource_id(
        "tree-cover-loss-by-driver", geostore_id, canopy_cover, dataset_version
    )

    # check if it exists
    await _get_resource(resource_id)
    link = DataMartResourceLink(
        link=f"{API_URL}/v0/land/tree-cover-loss-by-driver/{resource_id}"
    )
    return DataMartResourceLinkResponse(data=link)


@router.get(
    "/tree-cover-loss-by-driver/{resource_id}",
    response_class=ORJSONResponse,
    response_model=TreeCoverLossByDriverResponse,
    tags=["Land"],
    status_code=200,
)
async def tree_cover_loss_by_driver_get(
    *,
    resource_id: UUID = Path(..., title="Tree cover loss by driver ID"),
    response: Response,
    api_key: APIKey = Depends(get_api_key),
):
    """Retrieve a tree cover loss by drivers resource."""
    resource = await _get_resource(resource_id)

    if resource.status == AnalysisStatus.pending:
        response.headers["Retry-After"] = "1"

    tree_cover_loss_by_driver = TreeCoverLossByDriver.from_orm(resource)
    tree_cover_loss_by_driver_response = TreeCoverLossByDriverResponse(
        data=tree_cover_loss_by_driver
    )

    return tree_cover_loss_by_driver_response


@router.post(
    "/tree-cover-loss-by-driver",
    response_class=ORJSONResponse,
    response_model=DataMartResourceLinkResponse,
    tags=["Land"],
    status_code=202,
)
async def tree_cover_loss_by_driver_post(
    *,
    data: TreeCoverLossByDriverIn,
    background_tasks: BackgroundTasks,
    api_key: APIKey = Depends(get_api_key),
    request: Request,
):
    """Create new tree cover loss by drivers resource for a given geostore and
    canopy cover."""

    # check geostore is valid
    try:
        await get_geostore(data.geostore_id, GeostoreOrigin.rw)
    except HTTPException:
        raise HTTPException(
            status_code=422,
            detail=f"Geostore {data.geostore_id} can't be found or is not valid.",
        )

    # create initial Job item as pending
    # trigger background task to create item
    # return 202 accepted
    dataset_version = DEFAULT_LAND_DATASET_VERSIONS | data.dataset_version
    resource_id = _get_resource_id(
        "tree-cover-loss-by-driver",
        data.geostore_id,
        data.canopy_cover,
        dataset_version,
    )

    await _save_pending_resource(resource_id, request.url.path, api_key)

    background_tasks.add_task(
        compute_tree_cover_loss_by_driver,
        resource_id,
        data.geostore_id,
        data.canopy_cover,
        dataset_version,
    )

    link = DataMartResourceLink(
        link=f"{API_URL}/v0/land/tree-cover-loss-by-driver/{resource_id}"
    )
    return DataMartResourceLinkResponse(data=link)


def _get_resource_id(path, geostore_id, canopy_cover, dataset_version):
    return uuid.uuid5(
        uuid.NAMESPACE_OID, f"{path}_{geostore_id}_{canopy_cover}_{dataset_version}"
    )


async def _get_resource(resource_id):
    try:
        resource = await datamart_crud.get_result(resource_id)
        return resource
    except RecordNotFoundError:
        raise HTTPException(
            status_code=404, detail="Resource not found, may require computation."
        )


async def _save_pending_resource(resource_id, endpoint, api_key):
    pending_resource = DataMartResource(
        id=resource_id,
        status=AnalysisStatus.pending,
        endpoint=endpoint,
        requested_by=api_key,
    )

    await datamart_crud.save_result(pending_resource)
