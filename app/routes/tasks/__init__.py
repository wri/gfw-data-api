from typing import List

from ...models.orm.tasks import Task as ORMTask
from ...models.pydantic.tasks import Task, TaskResponse, TasksResponse


def task_response(data: ORMTask) -> TaskResponse:
    """Assure that task responses are parsed correctly and include associated
    assets."""

    return TaskResponse(data=data)


async def tasks_response(tasks_orm: List[ORMTask]) -> TasksResponse:
    """Serialize ORM response."""
    data = [Task.from_orm(task) for task in tasks_orm]
    return TasksResponse(data=data)
