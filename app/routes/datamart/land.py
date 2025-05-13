"""Beta APIs to more easily access important land use/land cover data without
needing to directly query our low-level data management APIs."""

import json
import re
import uuid
from typing import Dict, Optional
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
from starlette.status import HTTP_400_BAD_REQUEST

from app.crud import datamart as datamart_crud
from app.errors import RecordNotFoundError
from app.models.enum.geostore import GeostoreOrigin
from app.models.pydantic.datamart import (
    AnalysisStatus,
    AreaOfInterest,
    DataMartResource,
    DataMartResourceLink,
    DataMartResourceLinkResponse,
    DataMartSource,
    TreeCoverLossByDriver,
    TreeCoverLossByDriverIn,
    TreeCoverLossByDriverMetadata,
    TreeCoverLossByDriverResponse,
    parse_area_of_interest,
)
from app.responses import CSVStreamingResponse
from app.settings.globals import API_URL
from app.tasks.datamart.land import (
    DEFAULT_LAND_DATASET_VERSIONS,
    compute_tree_cover_loss_by_driver,
)
from app.utils.geostore import get_geostore

from ...authentication.api_keys import get_api_key
from . import OPENAPI_EXTRA

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
    return resolve_mutually_exclusive_datasets(dataset_versions)


@router.get(
    "/tree_cover_loss_by_driver",
    response_class=ORJSONResponse,
    response_model=DataMartResourceLinkResponse,
    tags=["Beta Land"],
    status_code=200,
    openapi_extra=OPENAPI_EXTRA,
)
async def tree_cover_loss_by_driver_search(
    *,
    aoi: AreaOfInterest = Depends(parse_area_of_interest),
    canopy_cover: int = Query(30, alias="canopy_cover", title="Canopy cover percent"),
    dataset_versions: Optional[Dict[str, str]] = Depends(_parse_dataset_versions),
    api_key: APIKey = Depends(get_api_key),
):
    """Search if a resource exists for a given geostore and canopy cover."""
    resource_id = _get_resource_id(
        "tree_cover_loss_by_driver",
        json.loads(aoi.json(exclude_none=True)),
        canopy_cover,
        dataset_versions,
    )

    # check if it exists
    resource_exists = await _check_resource_exists(resource_id)
    if not resource_exists:
        raise HTTPException(
            status_code=404, detail="Resource not found, may require computation."
        )

    link = DataMartResourceLink(
        link=f"{API_URL}/v0/land/tree_cover_loss_by_driver/{resource_id}"
    )
    return DataMartResourceLinkResponse(data=link)


@router.get(
    "/tree_cover_loss_by_driver/{resource_id}",
    response_class=ORJSONResponse,
    response_model=TreeCoverLossByDriverResponse,
    tags=["Beta Land"],
    status_code=200,
    responses={
        200: {
            "content": {
                "text/csv": {
                    "example": '"umd_tree_cover_loss__year","tsc_tree_cover_loss_drivers__driver","area__ha"\r\n"2001","Permanent agriculture",10.0\r\n"2001","Hard commodities",12.0\r\n"2001","Shifting cultivation",7.0\r\n"2001","Forest management",93.4\r\n"2001","Wildfires",42.0\r\n"2001","Settlements and infrastructure",13.562\r\n"2001","Other natural disturbances",6.0\r\n'
                },
            },
            "description": "Returns either JSON or CSV representation based on the Accept header. CSV representation will only return tree cover loss year, driver, and area.",
        }
    },
)
async def tree_cover_loss_by_driver_get(
    *,
    resource_id: UUID = Path(..., title="Tree cover loss by driver ID"),
    request: Request,
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

    if request.headers.get("Accept", None) == "text/csv":
        response.headers["Content-Type"] = "text/csv"
        response.headers["Content-Disposition"] = "attachment"
        csv_data = tree_cover_loss_by_driver_response.to_csv()
        return CSVStreamingResponse(iter([csv_data.getvalue()]), download=True)

    return tree_cover_loss_by_driver_response


@router.post(
    "/tree_cover_loss_by_driver",
    response_class=ORJSONResponse,
    response_model=DataMartResourceLinkResponse,
    tags=["Beta Land"],
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

    dataset_version = resolve_mutually_exclusive_datasets(data.dataset_version)

    resource_id = _get_resource_id(
        "tree_cover_loss_by_driver",
        json.loads(data.aoi.json(exclude_none=True)),
        data.canopy_cover,
        dataset_version,
    )

    resource_exists = await _check_resource_exists(resource_id)
    if resource_exists:
        raise HTTPException(
            status_code=409,
            detail=f"Resource f{resource_id} already exists with those parameters.",
        )

    link = DataMartResourceLink(
        link=f"{API_URL}/v0/land/tree_cover_loss_by_driver/{resource_id}"
    )
    if data.aoi.type == "global":
        try:
            _ = await _get_resource(resource_id)
        except HTTPException:
            raise HTTPException(
                HTTP_400_BAD_REQUEST,
                detail="Global computation not supported for this dataset and pre-computed results are not available.",
            )

        return DataMartResourceLinkResponse(data=link)

    geostore_id = await data.aoi.get_geostore_id()
    # check geostore is valid
    try:
        await get_geostore(geostore_id, GeostoreOrigin.rw)
    except HTTPException:
        raise HTTPException(
            status_code=422,
            detail=f"Geostore {geostore_id} can't be found or is not valid.",
        )

    metadata = _get_metadata(data.aoi, data.canopy_cover, dataset_version)
    # create initial Job item as pending
    await _save_pending_resource(resource_id, metadata, request.url.path, api_key)

    # trigger background task to create item
    # return 202 accepted
    background_tasks.add_task(
        compute_tree_cover_loss_by_driver,
        resource_id,
        geostore_id,
        data.canopy_cover,
        dataset_version,
    )

    return DataMartResourceLinkResponse(data=link)


def _get_resource_id(path, aoi, canopy_cover, dataset_version):
    return uuid.uuid5(
        uuid.NAMESPACE_OID, f"{path}_{aoi}_{canopy_cover}_{dataset_version}"
    )


async def _get_resource(resource_id):
    try:
        resource = await datamart_crud.get_result(resource_id)
        return TreeCoverLossByDriver.from_orm(resource)
    except RecordNotFoundError:
        raise HTTPException(
            status_code=404, detail="Resource not found, may require computation."
        )


async def _check_resource_exists(resource_id) -> bool:
    try:
        await datamart_crud.get_result(resource_id)
        return True
    except RecordNotFoundError:
        return False


async def _save_pending_resource(resource_id, metadata, endpoint, api_key):
    pending_resource = DataMartResource(
        id=resource_id,
        metadata=metadata,
        status=AnalysisStatus.pending,
        endpoint=endpoint,
        requested_by=api_key,
        message="Resource is still processing, follow Retry-After header.",
    )

    await datamart_crud.save_result(pending_resource)


def _get_metadata(
    aoi: AreaOfInterest, canopy_cover: int, dataset_version: Dict[str, str]
) -> TreeCoverLossByDriverMetadata:
    sources = [
        DataMartSource(dataset=dataset, version=version)
        for dataset, version in dataset_version.items()
    ]
    return TreeCoverLossByDriverMetadata(
        aoi=aoi,
        canopy_cover=canopy_cover,
        sources=sources,
    )


def resolve_mutually_exclusive_datasets(dataset_versions):
    mutually_exclusive_datasets = {
        "wri_google_tree_cover_loss_drivers": "tsc_tree_cover_loss_drivers"
    }

    dataset_version = DEFAULT_LAND_DATASET_VERSIONS.copy()
    for d, v in dataset_versions.items():
        if d in mutually_exclusive_datasets:
            dataset_to_remove = mutually_exclusive_datasets[d]
            _ = dataset_version.pop(dataset_to_remove, None)
        dataset_version[d] = v
    return dataset_version
