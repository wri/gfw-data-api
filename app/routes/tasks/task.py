"""Tasks represent the steps performed during asset creation.

You can view a single tasks or all tasks associated with as specific
asset. Only _service accounts_ can create or update tasks.
"""
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.responses import ORJSONResponse

from ...application import ContextEngine, db
from ...crud import assets, tasks, versions
from ...errors import RecordAlreadyExistsError, RecordNotFoundError
from ...models.enum.assets import AssetStatus, is_database_asset, is_tile_cache_asset
from ...models.enum.change_log import ChangeLogStatus
from ...models.enum.versions import VersionStatus
from ...models.orm.assets import Asset as ORMAsset
from ...models.orm.queries.fields import fields
from ...models.orm.tasks import Task as ORMTask
from ...models.pydantic.assets import AssetCreateIn, AssetType
from ...models.pydantic.change_log import ChangeLog
from ...models.pydantic.creation_options import DynamicVectorTileCacheCreationOptions
from ...models.pydantic.metadata import FieldMetadata
from ...models.pydantic.tasks import TaskCreateIn, TaskResponse, TaskUpdateIn
from ...settings.globals import TILE_CACHE_URL
from ...tasks.assets import put_asset
from ...utils.tile_cache import redeploy_tile_cache_service
from .. import is_service_account
from . import task_response

router = APIRouter()


@router.get(
    "/{task_id}",
    response_class=ORJSONResponse,
    tags=["Tasks"],
    response_model=TaskResponse,
)
async def get_task(*, task_id: UUID = Path(...)) -> TaskResponse:
    """Get single tasks by task ID."""
    try:
        row = await tasks.get_task(task_id)
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return task_response(row)


@router.put(
    "/{task_id}",
    response_class=ORJSONResponse,
    tags=["Tasks"],
    response_model=TaskResponse,
)
async def create_task(
    *,
    task_id: UUID = Path(...),
    request: TaskCreateIn,
    is_service_account: bool = Depends(is_service_account),
) -> TaskResponse:
    """Create a task."""

    input_data = request.dict(exclude_none=True, by_alias=True)
    try:
        task_row = await tasks.create_task(task_id, **input_data)
    except (RecordAlreadyExistsError, RecordNotFoundError) as e:
        raise HTTPException(status_code=400, detail=str(e))

    return task_response(task_row)


@router.patch(
    "/{task_id}",
    response_class=ORJSONResponse,
    tags=["Tasks"],
    response_model=TaskResponse,
)
async def update_task(
    *,
    task_id: UUID = Path(...),
    request: TaskUpdateIn,
    is_authorized: bool = Depends(is_service_account),
) -> TaskResponse:
    """Update the status of a task.

    If the task has errored, or all tasks are complete, propagate status
    to asset and version rows.
    """

    input_data = request.dict(exclude_none=True, by_alias=True)
    task_row = await tasks.update_task(task_id, **input_data)

    asset_id = task_row.asset_id

    status: Optional[str] = None

    # check if any of the change logs indicate failure
    for change_log in request.change_log:
        status = change_log.status
        if change_log.status == ChangeLogStatus.failed:
            break

    if status and status == ChangeLogStatus.failed:
        await _set_failed(task_id, asset_id)

    elif status and status == ChangeLogStatus.success:
        await _check_completed(asset_id)

    else:
        raise HTTPException(
            status_code=400,
            detail="change log status must be either `success` or `failed`",
        )

    return task_response(task_row)


async def _set_failed(task_id: UUID, asset_id: UUID):
    """Set asset status to `failed`.

    If asset is default asset, also set version status to `failed`
    """
    now = datetime.now()

    status_change_log: ChangeLog = ChangeLog(
        date_time=now,
        status=ChangeLogStatus.failed,
        message="One or more tasks failed.",
        detail=f"Check task /meta/tasks/{task_id} for more detail",
    )

    asset_row: ORMAsset = await assets.update_asset(
        asset_id,
        status=AssetStatus.failed,
        change_log=[status_change_log.dict(by_alias=True)],
    )

    # For database tables, try to fetch list of fields and their types from PostgreSQL
    # and add them to metadata object.
    # This is still useful, even if asset creation failed, since it will help to debug possible errors.
    # Query returns empty list in case table does not exist.
    if asset_row.asset_type in [AssetType.database_table, AssetType.geo_database_table]:
        await _update_asset_field_metadata(
            asset_row.dataset, asset_row.version, asset_id,
        )

    # If default asset failed, we must version status also to failed.
    if asset_row.is_default:
        dataset, version = asset_row.dataset, asset_row.version

        await versions.update_version(
            dataset,
            version,
            status=VersionStatus.failed,
            change_log=[status_change_log.dict(by_alias=True)],
        )


async def _check_completed(asset_id: UUID):
    """Check if all tasks have completed.

    If yes, set asset status to `saved`. If asset is default asset, also
    set version status to `saved`.
    """
    now = datetime.now()

    all_task_rows: List[ORMTask] = await tasks.get_tasks(asset_id)
    all_finished = _all_finished(all_task_rows)

    status_change_log: ChangeLog = ChangeLog(
        date_time=now,
        status=ChangeLogStatus.success,
        message=f"Successfully created asset {asset_id}.",
    )

    if all_finished:
        asset_row: ORMAsset = await assets.update_asset(
            asset_id,
            status=AssetStatus.saved,
            change_log=[status_change_log.dict(by_alias=True)],
        )
        # For database tables, fetch list of fields and their types from PostgreSQL
        # and add them to metadata object
        # Check if creation options specify to register a dynamic vector tile cache asset
        if is_database_asset(asset_row.asset_type):
            asset_row = await _update_asset_field_metadata(
                asset_row.dataset, asset_row.version, asset_id,
            )

            await _register_dynamic_vector_tile_cache(
                asset_row.dataset, asset_row.version, asset_row.metadata
            )

        # If default asset, make sure, version is also set to saved
        if asset_row.is_default:
            dataset, version = asset_row.dataset, asset_row.version

            await versions.update_version(
                dataset,
                version,
                status=VersionStatus.saved,
                change_log=[status_change_log.dict(by_alias=True)],
            )

        if is_tile_cache_asset(asset_row.asset_type):
            # Force new deployment of tile cache service, to make sure new tile cache version is recognized
            await redeploy_tile_cache_service(asset_id)


def _all_finished(task_rows: List[ORMTask]) -> bool:
    """Loop over task list to check if all completed successfully."""
    all_finished = True

    for row in task_rows:
        if any(
            changelog["status"] == ChangeLogStatus.success
            for changelog in row.change_log
        ):
            continue
        else:
            all_finished = False
            break

    return all_finished


async def _get_field_metadata(dataset: str, version: str) -> List[Dict[str, Any]]:
    """Get field list for asset and convert into Metadata object."""
    async with ContextEngine("READ"):
        rows = await db.all(fields, dataset=dataset, version=version)
    field_metadata = list()

    for row in rows:
        metadata = FieldMetadata.from_orm(row)
        if metadata.field_name_ in ["geom", "geom_wm", "gfw_geojson", "gfw_bbox"]:
            metadata.is_filter = False
            metadata.is_feature_info = False
        metadata.field_alias = metadata.field_name_
        field_metadata.append(metadata.dict(by_alias=True))

    return field_metadata


async def _update_asset_field_metadata(dataset, version, asset_id) -> ORMAsset:
    """Update asset field metadata."""

    fields_metadata: List[Dict[str, Any]] = await _get_field_metadata(dataset, version)

    async with ContextEngine("WRITE"):
        return await assets.update_asset(asset_id, fields=fields_metadata)


async def _register_dynamic_vector_tile_cache(
    dataset: str, version: str, metadata: Dict[str, Any]
) -> None:
    """Register dynamic vector tile cache asset with version if required."""
    default_asset: ORMAsset = await assets.get_default_asset(
        dataset, version,
    )
    creation_options = DynamicVectorTileCacheCreationOptions()
    create_dynamic_vector_tile_cache: Optional[
        bool
    ] = default_asset.creation_options.get("create_dynamic_vector_tile_cache", None)
    if create_dynamic_vector_tile_cache:
        data = AssetCreateIn(
            asset_type=AssetType.dynamic_vector_tile_cache,
            asset_uri=f"{TILE_CACHE_URL}/{dataset}/{version}/dynamic/{{z}}/{{x}}/{{y}}.pbf",
            is_managed=True,
            creation_options=creation_options.dict(by_alias=True),
            fields=default_asset.fields,
            metadata={
                "min_zoom": creation_options.min_zoom,
                "max_zoom": creation_options.max_zoom,
            },
        )

        try:
            async with ContextEngine("WRITE"):
                asset_orm = await assets.create_asset(
                    dataset, version, **data.dict(by_alias=True)
                )

        except Exception as e:
            # In case creating the asset record fails we only log to version change log
            log = ChangeLog(
                date_time=datetime.now(),
                status=ChangeLogStatus.failed,
                message="Failed to create Dynamic Vector Tile Cache Asset",
                detail=str(e),
            )
            async with ContextEngine("WRITE"):
                await versions.update_version(
                    dataset, version, change_log=[log.dict(by_alias=True)]
                )
        else:
            # otherwise we run the asset pipeline (synchronously)
            await put_asset(
                AssetType.dynamic_vector_tile_cache,
                asset_orm.asset_id,
                dataset,
                version,
                data.dict(by_alias=True),
            )
