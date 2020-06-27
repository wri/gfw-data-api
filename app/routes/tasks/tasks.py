"""

Tasks represent the steps performed during asset creation.
You can view a single tasks or all tasks associated with as specific asset.
Only _service accounts_ can create or update tasks.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.responses import ORJSONResponse

from ...application import ContextEngine
from ...crud import assets, tasks, versions
from ...models.orm.assets import Asset as ORMAsset
from ...models.orm.queries.fields import fields
from ...models.orm.tasks import Task as ORMTask
from ...models.pydantic.assets import AssetType
from ...models.pydantic.change_log import ChangeLog
from ...models.pydantic.metadata import FieldMetadata
from ...models.pydantic.tasks import Task, TaskResponse, TasksResponse, TaskUpdateIn
from .. import is_service_account

router = APIRouter()


@router.get(
    "/{task_id}",
    response_class=ORJSONResponse,
    tags=["Tasks"],
    response_model=TaskResponse,
)
async def get_task(*, task_id: UUID = Path(...)) -> TaskResponse:
    """
    Get single tasks by task ID
    """
    row = await tasks.get_task(task_id)
    return _task_response(row)


@router.get(
    "assets/{asset_id}",
    response_class=ORJSONResponse,
    tags=["Tasks"],
    response_model=TasksResponse,
)
async def get_asset_tasks_root(*, asset_id: UUID = Path(...)) -> TasksResponse:
    """Get all Tasks for selected asset"""
    rows: List[ORMTask] = await tasks.get_tasks(asset_id)
    return await _tasks_response(rows)


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
    """
    Update the status of a task.
    If the task has errored, or all tasks are complete, propagate status
    to asset and version rows.
    """

    input_data = request.dict()
    task_row = await tasks.update_task(task_id, **input_data)

    asset_id = task_row.asset_id

    status: Optional[str] = None

    # check if any of the change logs indicate failure
    for change_log in request.change_log:
        status = change_log.status
        if change_log.status == "failed":
            break

    if status and status == "failed":
        await _set_failed(task_id, asset_id)

    elif status and status == "success":
        await _check_completed(asset_id)

    else:
        raise HTTPException(
            status_code=400,
            detail="change log status must be either `success` or `failed`",
        )

    return _task_response(task_row)


async def _set_failed(task_id: UUID, asset_id: UUID):
    """
    Set asset status to `failed`.
    If asset is default asset, also set version status to `failed`
    """
    now = datetime.now()

    status_change_log: ChangeLog = ChangeLog(
        date_time=now,
        status="failed",
        message="One or more tasks failed.",
        detail=f"Check task /meta/tasks/{task_id} for more detail",
    )

    asset_row: ORMAsset = await assets.update_asset(
        asset_id, status="failed", change_log=[status_change_log]
    )
    if asset_row.is_default:
        dataset, version = asset_row.dataset, asset_row.version

        await versions.update_version(
            dataset, version, status="failed", change_log=[status_change_log]
        )


async def _check_completed(asset_id: UUID):
    """
    Check if all tasks have completed.
    If yes, set asset status to `saved`.
    If asset is default asset, also set version status to `saved`.
    """
    now = datetime.now()

    all_task_rows: List[ORMTask] = await tasks.get_tasks(asset_id)
    all_finished = _all_finished(all_task_rows)

    status_change_log: ChangeLog = ChangeLog(
        date_time=now,
        status="success",
        message=f"Successfully created asset {asset_id}.",
    )

    if all_finished:
        asset_row = await assets.update_asset(
            asset_id, status="saved", change_log=[status_change_log]
        )
        if asset_row.asset_type == AssetType.database_table:
            await _update_asset_field_metadata(
                asset_row.dataset, asset_row.version, asset_id,
            )

        if asset_row.is_default:
            dataset, version = asset_row.dataset, asset_row.version

            await versions.update_version(
                dataset, version, status="saved", change_log=[status_change_log]
            )


def _all_finished(task_rows: List[ORMTask]) -> bool:
    """
    Loop over task list to check if all completed successfully
    """
    all_finished = True

    for row in task_rows:
        if any(changelog.status == "success" for changelog in row.change_log):
            continue
        else:
            all_finished = False
            break

    return all_finished


async def _get_field_metadata(dataset: str, version: str) -> List[Dict[str, Any]]:
    """Get field list for asset and convert into Metadata object."""
    async with ContextEngine("READ") as db:
        rows = await db.all(fields, dataset=dataset, version=version)
    field_metadata = list()

    for row in rows:
        metadata = FieldMetadata.from_orm(row)
        if metadata.field_name_ in ["geom", "geom_wm", "gfw_geojson", "gfw_bbox"]:
            metadata.is_filter = False
            metadata.is_feature_info = False
        metadata.field_alias = metadata.field_name_
        field_metadata.append(metadata.dict())

    return field_metadata


async def _update_asset_field_metadata(dataset, version, asset_id):
    """
    Update asset field metadata.
    """

    field_metadata: List[Dict[str, Any]] = await _get_field_metadata(dataset, version)
    metadata = {"fields_": field_metadata}

    async with ContextEngine("WRITE"):
        await assets.update_asset(asset_id, metadata=metadata)


def _task_response(data: ORMTask) -> TaskResponse:
    """Assure that task responses are parsed correctly and include associated assets."""

    return TaskResponse(data=data)


async def _tasks_response(tasks_orm: List[ORMTask]) -> TasksResponse:
    """Serialize ORM response."""
    data = [Task.from_orm(task) for task in tasks_orm]  # .dict(by_alias=True)
    return TasksResponse(data=data)
