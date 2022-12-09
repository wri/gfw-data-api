from typing import List
from uuid import UUID

from asyncpg import ForeignKeyViolationError, UniqueViolationError
from fastapi.encoders import jsonable_encoder
from sqlalchemy import func

from ..errors import RecordAlreadyExistsError, RecordNotFoundError
from ..models.orm.tasks import Task as ORMTask
from . import update_data


async def count_filtered_tasks_fn(asset_id):
    query = ORMTask.query.where(ORMTask.asset_id == asset_id).order_by(
        ORMTask.created_on
    )

    async def count_assets() -> int:
        return await func.count().select().select_from(query.alias()).gino.scalar()

    return count_assets


async def get_filtered_tasks_fn(asset_id):
    query = ORMTask.query.where(ORMTask.asset_id == asset_id).order_by(
        ORMTask.created_on
    )

    async def paginated_assets(size: int = None, offset: int = 0) -> List[ORMTask]:
        return await query.limit(size).offset(offset).gino.load(ORMTask).all()

    return paginated_assets


async def get_tasks(asset_id: UUID) -> List[ORMTask]:
    tasks: List[ORMTask] = (
        await ORMTask.query.where(ORMTask.asset_id == asset_id)
        .order_by(ORMTask.created_on)
        .gino.all()
    )

    return tasks


async def get_task(task_id: UUID) -> ORMTask:
    row: ORMTask = await ORMTask.get(task_id)
    if row is None:
        raise RecordNotFoundError(f"Task with task_id {task_id} does not exist.")
    return row


async def create_task(task_id: UUID, **data) -> ORMTask:
    jsonable_data = jsonable_encoder(data)
    try:
        new_task: ORMTask = await ORMTask.create(task_id=task_id, **jsonable_data)
    except UniqueViolationError:
        raise RecordAlreadyExistsError(f"Task with task_id {task_id} already exists.")
    except ForeignKeyViolationError:
        raise RecordNotFoundError(f"Asset {jsonable_data['asset_id']} does not exist.")
    return new_task


async def create_or_update_task(task_id: UUID, **data) -> ORMTask:
    try:
        new_task: ORMTask = await ORMTask.create(task_id=task_id, **data)
        return new_task
    except UniqueViolationError:
        row: ORMTask = await get_task(task_id)
        return await update_data(row, data)


async def update_task(task_id: UUID, **data) -> ORMTask:
    jsonable_data = jsonable_encoder(data)
    row: ORMTask = await get_task(task_id)
    return await update_data(row, jsonable_data)


async def delete_task(task_id: UUID) -> ORMTask:
    row: ORMTask = await get_task(task_id)
    await ORMTask.delete.where(ORMTask.task_id == task_id).gino.status()

    return row
