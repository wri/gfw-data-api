"""Beta APIs to more easily access important land use/land cover data without
needing to directly query our low-level data management APIs."""

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
from pydantic import ValidationError
from starlette.status import HTTP_400_BAD_REQUEST


from app.crud import datamart as datamart_crud
from app.errors import RecordNotFoundError
from app.models.enum.geostore import GeostoreOrigin
from app.models.pydantic.datamart import (
    AdminAreaOfInterest,
    AnalysisStatus,
    AreaOfInterest,
    DataMartResource,
    DataMartResourceLink,
    DataMartResourceLinkResponse,
    DataMartSource,
    GeostoreAreaOfInterest,
    TreeCoverLossByDriver,
    TreeCoverLossByDriverIn,
    TreeCoverLossByDriverMetadata,
    TreeCoverLossByDriverResponse,
    Global,
)
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
    return DEFAULT_LAND_DATASET_VERSIONS | dataset_versions


def _parse_area_of_interest(request: Request) -> AreaOfInterest:
    params = request.query_params
    aoi_type = params.get("aoi[type]")
    try:
        if aoi_type == "geostore":
            return GeostoreAreaOfInterest(
                geostore_id=params.get("aoi[geostore_id]", None)
            )

            # Otherwise, check if the request contains admin area information
        if aoi_type == "admin":
            return AdminAreaOfInterest(
                country=params.get("aoi[country]", None),
                region=params.get("aoi[region]", None),
                subregion=params.get("aoi[subregion]", None),
                provider=params.get("aoi[provider]", None),
                version=params.get("aoi[version]", None),
            )

        if aoi_type == "global":
            return Global()

        # If neither type is provided, raise an error
        raise HTTPException(
            status_code=422, detail="Invalid Area of Interest parameters"
        )
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors())


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
    aoi: AreaOfInterest = Depends(_parse_area_of_interest),
    canopy_cover: int = Query(30, alias="canopy_cover", title="Canopy cover percent"),
    dataset_versions: Optional[Dict[str, str]] = Depends(_parse_dataset_versions),
    api_key: APIKey = Depends(get_api_key),
):
    """Search if a resource exists for a given geostore and canopy cover."""
    resource_id = _get_resource_id(
        "tree_cover_loss_by_driver", aoi, canopy_cover, dataset_versions
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

    dataset_version = DEFAULT_LAND_DATASET_VERSIONS | data.dataset_version
    resource_id = _get_resource_id(
        "tree_cover_loss_by_driver",
        data.aoi,
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
