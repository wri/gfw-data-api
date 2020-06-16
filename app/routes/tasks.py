from typing import Any, Dict, List

from fastapi import APIRouter, BackgroundTasks, Depends, Response
from fastapi.responses import ORJSONResponse

from ..crud import tasks
from ..models.orm.tasks import Task as ORMTask
from ..models.pydantic.tasks import Task, TaskUpdateIn
from ..routes import dataset_dependency, is_admin, version_dependency
from ..tasks.default_assets import create_default_asset

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
    """Create or update the status of a task."""

    # input_data = request.dict()
    result_task: ORMTask = await tasks.update_task(task_id, **(task.dict()))

    return result_task
