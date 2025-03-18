"""Run analysis on registered datasets."""

import re
import uuid
from typing import Dict, List, Optional
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
from app.models.pydantic.datamart import (
    AnalysisStatus,
    DataMartResource,
    DataMartResourceLink,
    DataMartResourceLinkResponse,
    TreeCoverLossByDriver,
    TreeCoverLossByDriverIn,
    TreeCoverLossByDriverResponse,
    AreaOfInterest,
    GeostoreAreaOfInterest,
    AdminAreaOfInterest,
)
from app.settings.globals import API_URL
from app.tasks.datamart.land import (
    DEFAULT_LAND_DATASET_VERSIONS,
    compute_tree_cover_loss_by_driver,
)
from app.utils.geostore import get_geostore

from ...authentication.api_keys import get_api_key

router = APIRouter()

def _parse_dataset_versions(request: Request) -> Dict[str, str]:
    dataset_versions = {}
    errors = []
    for key in request.query_params.keys():
        if key.startswith("dataset_version"):
            matches = re.findall(r"dataset_version\[([a-z][a-z0-9_-]*)\]$", key)
            if len(matches) == 1:
                dataset_versions[matches[0]] = request.query_params[key]
            else:
                errors.append(key)

    if errors:
        raise HTTPException(
            status_code=422,
            detail=f"Could not parse the following malformed dataset_version parameters: {errors}",
        )

    # Merge dataset version overrides with default dataset versions
    return DEFAULT_LAND_DATASET_VERSIONS | dataset_versions


def _parse_area_of_interest(request: Request) -> AreaOfInterest:
    if 'aoi[geostore_id]' in request.query_params:
        return GeostoreAreaOfInterest(geostore_id=request.query_params['aoi[geostore_id]'])

        # Otherwise, check if the request contains admin area information
    if 'aoi[country]' in request.query_params:
        return AdminAreaOfInterest(
            country=request.query_params['aoi[country]'],
            region=request.query_params.get('aoi[region]'),
            subregion=request.query_params.get('aoi[subregion]'),
            provider=request.query_params.get('aoi[provider]'),
            version=request.query_params.get('aoi[version]'),
        )

    # If neither type is provided, raise an error
    raise HTTPException(status_code=422, detail="Invalid Area of Interest parameters")


@router.get(
    "/tree_cover_loss_by_driver",
    response_class=ORJSONResponse,
    response_model=DataMartResourceLinkResponse,
    tags=["Land"],
    status_code=200,
    openapi_extra={
        "parameters": [
            {
                "name": "aoi",
                "in": "query",
                "required": True,
                "style": "deepObject",
                "explode": True,
                "example": {
                    "geostore_id": "637d378f-93a9-4364-bfa8-95b6afd28c3a",
                },
                "description": "The Area of Interest",
                "schema": {
                    "oneOf": [
                        {"$ref": "#/components/schemas/GeostoreAreaOfInterest"},
                        {"$ref": "#/components/schemas/AdminAreaOfInterest"},
                    ]
                }
            },
            {
                "name": "dataset_version",
                "in": "query",
                "required": False,
                "style": "deepObject",
                "explode": True,
                "schema": {
                    "type": "object",
                    "additionalProperties": {"type": "string"},
                },
                "example": {
                    "umd_tree_cover_loss": "v1.11",
                    "tsc_tree_cover_loss_drivers": "v2023",
                },
                "description": (
                        "Pass dataset version overrides as bracketed query parameters.",
                )
            }
        ]
    },
)
async def tree_cover_loss_by_driver_search(
    *,
    aoi: AreaOfInterest = Depends(_parse_area_of_interest),
    canopy_cover: int = Query(30, alias="canopy_cover", title="Canopy cover percent"),
    dataset_versions: Optional[Dict[str, str]] = Depends(_parse_dataset_versions),
    api_key: APIKey = Depends(get_api_key),
):
    """Search if a resource exists for a given geostore and canopy cover."""
    geostore_id = await aoi.get_geostore_id()
    resource_id = _get_resource_id(
        "tree_cover_loss_by_driver", geostore_id, canopy_cover, dataset_versions
    )

    # check if it exists
    await _get_resource(resource_id)
    link = DataMartResourceLink(
        link=f"{API_URL}/v0/land/tree_cover_loss_by_driver/{resource_id}"
    )
    return DataMartResourceLinkResponse(data=link)


@router.get(
    "/tree_cover_loss_by_driver/{resource_id}",
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
    tree_cover_loss_by_driver = await _get_resource(resource_id)

    if tree_cover_loss_by_driver.status == AnalysisStatus.pending:
        response.headers["Retry-After"] = "1"

    tree_cover_loss_by_driver_response = TreeCoverLossByDriverResponse(
        data=tree_cover_loss_by_driver
    )

    return tree_cover_loss_by_driver_response


@router.post(
    "/tree_cover_loss_by_driver",
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
    geostore_id = await data.aoi.get_geostore_id()

    # check geostore is valid
    try:
        await get_geostore(geostore_id, GeostoreOrigin.rw)
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
        "tree_cover_loss_by_driver",
        geostore_id,
        data.canopy_cover,
        dataset_version,
    )

    await _save_pending_resource(resource_id, request.url.path, api_key)

    background_tasks.add_task(
        compute_tree_cover_loss_by_driver,
        resource_id,
        geostore_id,
        data.canopy_cover,
        dataset_version,
    )

    link = DataMartResourceLink(
        link=f"{API_URL}/v0/land/tree_cover_loss_by_driver/{resource_id}"
    )
    return DataMartResourceLinkResponse(data=link)


def _get_resource_id(path, geostore_id, canopy_cover, dataset_version):
    return uuid.uuid5(
        uuid.NAMESPACE_OID, f"{path}_{geostore_id}_{canopy_cover}_{dataset_version}"
    )


async def _get_resource(resource_id):
    try:
        resource = await datamart_crud.get_result(resource_id)
        return TreeCoverLossByDriver.from_orm(resource)
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
        message="Resource is still processing, follow Retry-After header.",
    )

    await datamart_crud.save_result(pending_resource)
