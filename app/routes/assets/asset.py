"""Assets are replicas of the original source files.

Assets might be served in different formats, attribute values might be
altered, additional attributes added, and feature resolution might have
changed. Assets are either managed or unmanaged. Managed assets are
created by the API and users can rely on data integrity. Unmanaged
assets are only loosely linked to a dataset version and users must
cannot rely on full integrity. We can only assume that unmanaged are
based on the same version and do not know the processing history.
"""
from typing import List
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Path
from fastapi.responses import ORJSONResponse

from ...crud import assets, tasks
from ...errors import RecordNotFoundError
from ...models.enum.assets import is_database_asset
from ...models.orm.assets import Asset as ORMAsset
from ...models.orm.tasks import Task as ORMTask
from ...models.pydantic.assets import AssetResponse, AssetType, AssetUpdateIn
from ...models.pydantic.change_log import ChangeLog, ChangeLogResponse
from ...models.pydantic.creation_options import (
    CreationOptions,
    CreationOptionsResponse,
    creation_option_factory,
)
from ...models.pydantic.metadata import FieldMetadata, FieldMetadataResponse
from ...models.pydantic.statistics import Stats, StatsResponse
from ...models.pydantic.tasks import TasksResponse
from ...routes import is_admin
from ...tasks.delete_assets import (
    delete_database_table,
    delete_dynamic_vector_tile_cache_assets,
    delete_raster_tileset_assets,
    delete_static_raster_tile_cache_assets,
    delete_static_vector_tile_cache_assets,
)
from ..assets import asset_response
from ..tasks import tasks_response

router = APIRouter()


@router.get(
    "/{asset_id}",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=AssetResponse,
)
async def get_asset(*, asset_id: UUID = Path(...),) -> AssetResponse:
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
            row.creation_options.implementation,
        )

    elif row.asset_type == AssetType.static_vector_tile_cache:
        background_tasks.add_task(
            delete_static_vector_tile_cache_assets,
            row.dataset,
            row.version,
            row.creation_options.implementation,
        )

    elif row.asset_type == AssetType.static_raster_tile_cache:
        background_tasks.add_task(
            delete_static_raster_tile_cache_assets,
            row.dataset,
            row.version,
            row.creation_options.implementation,
        )

    elif row.asset_type == AssetType.raster_tile_set:
        background_tasks.add_task(
            delete_raster_tileset_assets,
            row.dataset,
            row.version,
            row.creation_options.srid,
            row.creation_options.size,
            row.creation_options.col,
            row.creation_options.value,
        )
    elif is_database_asset(row.asset_type):
        background_tasks.add_task(delete_database_table, row.dataset, row.version)
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
    response_model=TasksResponse,
)
async def get_tasks(*, asset_id: UUID = Path(...)) -> TasksResponse:
    """Get all Tasks for selected asset."""
    rows: List[ORMTask] = await tasks.get_tasks(asset_id)
    return await tasks_response(rows)


@router.get(
    "/{asset_id}/change_log",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=ChangeLogResponse,
)
async def get_change_log(asset_id: UUID = Path(...)):
    asset = await assets.get_asset(asset_id)
    change_logs: List[ChangeLog] = [
        ChangeLog(**change_log) for change_log in asset.change_log
    ]

    return ChangeLogResponse(data=change_logs)


@router.get(
    "/{asset_id}/creation_options",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=CreationOptionsResponse,
)
async def get_creation_options(asset_id: UUID = Path(...)):
    asset = await assets.get_asset(asset_id)
    creation_options: CreationOptions = creation_option_factory(
        asset.asset_type, asset.creation_options
    )
    return CreationOptionsResponse(data=creation_options)


@router.get(
    "/{asset_id}/stats",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=StatsResponse,
)
async def get_stats(asset_id: UUID = Path(...)):
    asset = await assets.get_asset(asset_id)
    stats: Stats = Stats(**asset.stats)
    return StatsResponse(data=stats)


@router.get(
    "/{asset_id}/fields",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=FieldMetadataResponse,
)
async def get_fields(asset_id: UUID = Path(...)):
    asset = await assets.get_asset(asset_id)
    fields: List[FieldMetadata] = [FieldMetadata(**field) for field in asset.fields]

    return FieldMetadataResponse(data=fields)
