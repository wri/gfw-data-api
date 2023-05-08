"""Assets are replicas of the original source files.

Assets might be served in different formats, attribute values might be
altered, additional attributes added, and feature resolution might have
changed. Assets are either managed or unmanaged. Managed assets are
created by the API and users can rely on data integrity. Unmanaged
assets are only loosely linked to a dataset version and users must
cannot rely on full integrity. We can only assume that unmanaged are
based on the same version and do not know the processing history.
"""
from typing import List, Optional, Union
from uuid import UUID

# from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Path, status
from fastapi.responses import ORJSONResponse
from starlette.responses import JSONResponse

from app.models.pydantic.responses import Response
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    HTTPException,
    Path,
    Query,
    Request,
    status,
)

from app.settings.globals import API_URL

from ...authentication.token import is_admin
from ...crud import assets
from ...crud import metadata as metadata_crud
from ...crud import tasks
from ...errors import RecordAlreadyExistsError, RecordNotFoundError
from ...models.enum.assets import is_database_asset, is_single_file_asset
from ...models.orm.asset_metadata import FieldMetadata as ORMFieldMetadata
from ...models.orm.assets import Asset as ORMAsset
from ...models.orm.tasks import Task as ORMTask
from ...models.pydantic.asset_metadata import (
    AssetMetadata,
    AssetMetadataResponse,
    AssetMetadataUpdate,
    FieldMetadataResponse,
    FieldMetadataUpdate,
    FieldsMetadataResponse,
    asset_metadata_factory,
)
from ...models.pydantic.assets import AssetResponse, AssetType, AssetUpdateIn
from ...models.pydantic.change_log import ChangeLog, ChangeLogResponse
from ...models.pydantic.creation_options import (
    CreationOptions,
    CreationOptionsResponse,
    creation_option_factory,
)
from ...models.pydantic.extent import Extent, ExtentResponse
from ...models.pydantic.statistics import Stats, StatsResponse, stats_factory
from ...models.pydantic.tasks import PaginatedTasksResponse, TasksResponse
from ...tasks.delete_assets import (
    delete_database_table_asset,
    delete_dynamic_vector_tile_cache_assets,
    delete_raster_tile_cache_assets,
    delete_raster_tileset_assets,
    delete_single_file_asset,
    delete_static_vector_tile_cache_assets,
)
from ...utils.paginate import paginate_collection
from ...utils.path import infer_srid_from_grid
from ..assets import asset_response
from ..tasks import paginated_tasks_response, tasks_response

router = APIRouter()


@router.get(
    "/{asset_id}",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=AssetResponse,
)
async def get_asset(
    *,
    asset_id: UUID = Path(...),
) -> AssetResponse:
    """Get a specific asset."""
    try:
        row: ORMAsset = await assets.get_asset(asset_id)
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return await asset_response(row)


@router.patch(
    "/{asset_id}",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=AssetResponse,
)
async def update_asset(
    *,
    asset_id: UUID = Path(...),
    request: AssetUpdateIn,
    is_authorized: bool = Depends(is_admin),
) -> AssetResponse:
    """Update Asset metadata."""

    input_data = request.dict(exclude_none=True, by_alias=True)

    try:
        row: ORMAsset = await assets.update_asset(asset_id, **input_data)
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except NotImplementedError as e:
        raise HTTPException(status_code=501, detail=str(e))

    return await asset_response(row)


@router.delete(
    "/{asset_id}",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=AssetResponse,
)
async def delete_asset(
    *,
    asset_id: UUID = Path(...),
    is_authorized: bool = Depends(is_admin),
    background_tasks: BackgroundTasks,
) -> AssetResponse:
    """Delete selected asset.

    For managed assets, all resources will be deleted. For non-managed
    assets, only the link will be deleted.
    """

    try:
        row: ORMAsset = await assets.get_asset(asset_id)
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if row.is_default:
        raise HTTPException(
            status_code=409,
            detail="Deletion failed. You cannot delete a default asset. "
            "To delete a default asset you must delete the parent version.",
        )

    if row.asset_type == AssetType.dynamic_vector_tile_cache:
        background_tasks.add_task(
            delete_dynamic_vector_tile_cache_assets,
            row.dataset,
            row.version,
            row.creation_options["implementation"],
        )

    elif row.asset_type == AssetType.static_vector_tile_cache:
        background_tasks.add_task(
            delete_static_vector_tile_cache_assets,
            row.dataset,
            row.version,
            row.creation_options["implementation"],
        )

    elif row.asset_type == AssetType.raster_tile_cache:
        background_tasks.add_task(
            delete_raster_tile_cache_assets,
            row.dataset,
            row.version,
            row.creation_options.get("implementation", "default"),
        )

    elif row.asset_type == AssetType.raster_tile_set:
        grid = row.creation_options["grid"]
        background_tasks.add_task(
            delete_raster_tileset_assets,
            row.dataset,
            row.version,
            infer_srid_from_grid(grid),
            grid,
            row.creation_options["pixel_meaning"],
        )
    elif is_database_asset(row.asset_type):
        background_tasks.add_task(delete_database_table_asset, row.dataset, row.version)
    elif is_single_file_asset(row.asset_type):
        background_tasks.add_task(delete_single_file_asset, row.asset_uri)
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot delete asset of type {row.asset_type}. Not implemented.",
        )

    row = await assets.delete_asset(asset_id)

    return await asset_response(row)


@router.get(
    "/{asset_id}/tasks",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=Union[PaginatedTasksResponse, TasksResponse],
)
async def get_tasks(
    *,
    asset_id: UUID = Path(...),
    request: Request,
    page_number: Optional[int] = Query(
        default=None, alias="page[number]", ge=1, description="The page number."
    ),
    page_size: Optional[int] = Query(
        default=None,
        alias="page[size]",
        ge=1,
        description="The number of tasks per page. Default is `10`.",
    ),
) -> Union[PaginatedTasksResponse, TasksResponse]:
    """Get all Tasks for selected asset.

    Will attempt to paginate if `page[size]` or `page[number]` is
    provided. Otherwise, it will attempt to return the entire list of
    tasks in the response.
    """

    if page_number or page_size:
        try:
            data, links, meta = await paginate_collection(
                paged_items_fn=await tasks.get_filtered_tasks_fn(asset_id),
                item_count_fn=await tasks.count_filtered_tasks_fn(asset_id),
                request_url=f"{API_URL}{request.url.path}",
                page=page_number,
                size=page_size,
            )

            return await paginated_tasks_response(
                tasks_orm=data, links=links, meta=meta
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    rows: List[ORMTask] = await tasks.get_tasks(asset_id)
    return await tasks_response(rows)


@router.get(
    "/{asset_id}/change_log",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=ChangeLogResponse,
)
async def get_change_log(asset_id: UUID = Path(...)) -> ChangeLogResponse:
    asset: ORMAsset = await assets.get_asset(asset_id)
    change_logs: List[ChangeLog] = [
        ChangeLog(**change_log) for change_log in asset.change_log
    ]

    return ChangeLogResponse(data=change_logs)


@router.get(
    "/{asset_id}/creation_options",
    response_class=JSONResponse,
    tags=["Assets"],
    response_model=CreationOptionsResponse,
)
async def get_creation_options(asset_id: UUID = Path(...)):
    # Not using ORJSONResponse because orjson won't serialize the numeric
    # keys in a Symbology object
    asset: ORMAsset = await assets.get_asset(asset_id)
    creation_options: CreationOptions = creation_option_factory(
        asset.asset_type, asset.creation_options
    )
    return CreationOptionsResponse(data=creation_options)


@router.get(
    "/{asset_id}/extent",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=ExtentResponse,
)
async def get_extent(asset_id: UUID = Path(...)):
    asset: ORMAsset = await assets.get_asset(asset_id)
    extent: Optional[Extent] = asset.extent
    return ExtentResponse(data=extent)


@router.get(
    "/{asset_id}/stats",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=StatsResponse,
)
async def get_stats(asset_id: UUID = Path(...)):
    asset: ORMAsset = await assets.get_asset(asset_id)
    stats: Optional[Stats] = stats_factory(asset.asset_type, asset.stats)
    return StatsResponse(data=stats)


@router.get(
    "/{asset_id}/fields",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=FieldsMetadataResponse,
)
async def get_fields(asset_id: UUID = Path(...)):
    asset: ORMAsset = await assets.get_asset(asset_id)
    fields = await metadata_crud.get_asset_fields_dicts(asset)

    return FieldsMetadataResponse(data=fields)


@router.get(
    "/{asset_id}/fields/{field_name}",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=FieldMetadataResponse,
)
async def get_field_metadata(*, asset_id: UUID = Path(...), field_name: str):
    metadata = await metadata_crud.get_asset_metadata(asset_id)

    try:
        field_metadata: ORMFieldMetadata = await metadata_crud.get_asset_field(
            metadata.id, field_name
        )
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return FieldMetadataResponse(data=field_metadata)


@router.patch(
    "/{asset_id}/fields/{field_name}",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=FieldMetadataResponse,
)
async def update_field_metadata(
    *, asset_id: UUID = Path(...), field_name: str, request: FieldMetadataUpdate
):
    input_data = request.dict(exclude_none=True, by_alias=True)
    metadata = await metadata_crud.get_asset_metadata(asset_id)
    field_metadata: ORMFieldMetadata = await metadata_crud.update_field_metadata(
        metadata.id, field_name, **input_data
    )

    return FieldMetadataResponse(data=field_metadata)


@router.get(
    "/{asset_id}/metadata",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=AssetMetadataResponse,
)
async def get_metadata(asset_id: UUID = Path(...)):
    try:
        asset = await assets.get_asset(asset_id)
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    validated_metadata = asset_metadata_factory(asset)

    return Response(data=validated_metadata)


@router.post(
    "/{asset_id}/metadata",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=AssetMetadataResponse,
)
async def create_metadata(*, asset_id: UUID = Path(...), request: AssetMetadata):
    input_data = request.dict(exclude_none=True, by_alias=True)
    asset = await assets.get_asset(asset_id)

    try:
        asset.metadata = await metadata_crud.create_asset_metadata(
            asset_id, **input_data
        )
    except RecordAlreadyExistsError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))

    validated_metadata = asset_metadata_factory(asset)

    return Response(data=validated_metadata)


@router.patch(
    "/{asset_id}/metadata",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=AssetMetadataResponse,
)
async def update_metadata(*, asset_id: UUID = Path(...), request: AssetMetadataUpdate):

    input_data = request.dict(exclude_none=True, by_alias=True)

    try:
        asset = await assets.get_asset(asset_id)
        asset.metadata = await metadata_crud.update_asset_metadata(
            asset_id, **input_data
        )
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    validated_metadata = asset_metadata_factory(asset)

    return Response(data=validated_metadata)


@router.delete(
    "/{asset_id}/metadata",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=AssetMetadataResponse,
)
async def delete_metadata(asset_id: UUID = Path(...)):

    try:
        asset = await assets.get_asset(asset_id)
        asset.metadata = await metadata_crud.delete_asset_metadata(asset_id)
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    validated_metadata = asset_metadata_factory(asset)

    return Response(data=validated_metadata)
