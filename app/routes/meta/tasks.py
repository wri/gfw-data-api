"""

Tasks represent the steps performed during asset creation.
"""

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import ORJSONResponse

from ...crud import assets, tasks, versions
from ...models.orm.assets import Asset as ORMAsset
from ...models.orm.tasks import Task as ORMTask
from ...models.pydantic.change_log import ChangeLog
from ...models.pydantic.tasks import TaskResponse, TaskUpdateIn
from ...routes import is_service_account

router = APIRouter()


@router.get(
    "/tasks/{task_id}",
    response_class=ORJSONResponse,
    tags=["Task"],
    response_model=TaskResponse,
)
async def get_task(*, task_id) -> TaskResponse:
    row = await tasks.get_task(task_id)
    return _task_response(row)


@router.patch(
    "/tasks/{task_id}",
    response_class=ORJSONResponse,
    tags=["Task"],
    response_model=TaskResponse,
)
async def update_task(
    *,
    task_id: UUID,
    request: TaskUpdateIn,
    is_service_account: bool = Depends(is_service_account),
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


def _task_response(data: ORMTask) -> TaskResponse:
    """Assure that task responses are parsed correctly and include associated assets."""

    return TaskResponse(data=data)
