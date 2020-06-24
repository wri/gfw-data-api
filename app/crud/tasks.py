from typing import Any, List, Optional
from uuid import UUID

from asyncpg import UniqueViolationError
from fastapi import HTTPException

from ..models.orm.tasks import Task as ORMTask
from . import update_data


async def get_tasks(asset_id: UUID) -> List[ORMTask]:
    tasks: List[ORMTask] = await ORMTask.query.where(
        ORMTask.asset_id == asset_id
    ).gino.all()

    return tasks


async def get_task(task_id: UUID) -> ORMTask:
    row: ORMTask = await ORMTask.get(task_id)
    if row is None:
        raise HTTPException(
            status_code=404, detail=f"Task with task_id {task_id} does not exist",
        )
    return row


async def create_task(task_id: UUID, **data) -> ORMTask:
    try:
        new_task: ORMTask = await ORMTask.create(task_id=task_id, **data)
    except UniqueViolationError:
        raise HTTPException(
            status_code=400, detail=f"Task with task_id {task_id} already exists",
        )

    return new_task


async def create_or_update_task(task_id: UUID, **data) -> ORMTask:
    try:
        new_task: ORMTask = await ORMTask.create(task_id=task_id, **data)
        return new_task
    except UniqueViolationError:
        row: ORMTask = await get_task(task_id)
        return await update_data(row, data)


async def update_task(task_id: UUID, **data) -> ORMTask:
    row: ORMTask = await get_task(task_id)
    return await update_data(row, data)


async def delete_task(task_id: UUID) -> ORMTask:
    row: ORMTask = await get_task(task_id)
    await ORMTask.delete.where(ORMTask.task_id == task_id).gino.status()

    return row
