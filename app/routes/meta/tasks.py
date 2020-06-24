"""

Tasks represent the steps performed during asset creation.
"""


from datetime import datetime
from typing import Any, Dict, List

from fastapi import APIRouter, BackgroundTasks, Depends, Response
from fastapi.responses import ORJSONResponse

from app.crud import assets, tasks, versions
from app.models.orm.assets import Asset as ORMAsset
from app.models.orm.tasks import Task as ORMTask
from app.models.orm.versions import Version as ORMVersion
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.tasks import Task, TaskUpdateIn
from app.routes import dataset_dependency, is_admin, version_dependency
from app.tasks.default_assets import create_default_asset

router = APIRouter()


@router.put(
    "/tasks/{task_id}",
    response_class=ORJSONResponse,
    tags=["Task"],
    response_model=Task,
    status_code=202,
)
async def update_task(
    *,
    task_id: str,
    # version: str = Depends(version_dependency),
    task: TaskUpdateIn,
    # is_authorized: bool = Depends(is_admin),
    response: Response,
):
    """
    Update the status of a task.
    If the task has errored, or all tasks are complete, propagate status
    to asset and version rows.
    """
    task_row = await tasks.update_task(task_id, **(task.dict()))

    asset_id = task_row.asset_id
    all_task_rows = await tasks.get_tasks(asset_id)

    now = datetime.now()

    if task.change_log[0].status == "failed":
        asset_row: ORMAsset = await assets.update_asset(
            asset_id, status="failed", change_log=[task.change_log]
        )
        dataset, version = asset_row.dataset, asset_row.version
        version_changelog = ChangeLog(
            date_time=now, status="failed", message="One or more tasks failed"
        )
        await versions.update_version(
            dataset, version, status="failed", change_log=[version_changelog]
        )

    elif task.change_log[0].status == "success":
        all_finished = True
        for row in all_task_rows:
            if any(changelog.status == "success" for changelog in row.change_log):
                continue
            else:
                all_finished = False
                break
        if all_finished:
            asset_row = await assets.update_asset(
                asset_id, status="success", change_log=[task.change_log]
            )
            dataset, version = asset_row.dataset, asset_row.version
            version_changelog = ChangeLog(
                date_time=now,
                status="success",
                message="All tasks completed successfully",
            )
            await versions.update_version(
                dataset, version, status="success", change_log=[version_changelog]
            )

    return task_row
