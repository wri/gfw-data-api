from typing import List

from ...models.orm.tasks import Task as ORMTask
from ...models.pydantic.responses import PaginationLinks, PaginationMeta
from ...models.pydantic.tasks import (
    PaginatedTasksResponse,
    Task,
    TaskResponse,
    TasksResponse,
)


def task_response(data: ORMTask) -> TaskResponse:
    """Assure that task responses are parsed correctly and include associated
    assets."""

    return TaskResponse(data=data)


async def tasks_response(tasks_orm: List[ORMTask]) -> TasksResponse:
    """Serialize ORM response."""
    data = [Task.from_orm(task) for task in tasks_orm]
    return TasksResponse(data=data)


async def paginated_tasks_response(
    tasks_orm: List[ORMTask], links: PaginationLinks, meta: PaginationMeta
) -> PaginatedTasksResponse:
    """Serialize ORM response."""
    data = [Task.from_orm(task) for task in tasks_orm]
    return PaginatedTasksResponse(data=data, links=links, meta=meta)
