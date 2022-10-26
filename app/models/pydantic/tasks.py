from typing import List
from uuid import UUID

from .base import BaseRecord, StrictBaseModel
from .change_log import ChangeLog
from .responses import PaginationLinks, PaginationMeta, Response


class Task(BaseRecord):
    task_id: UUID
    asset_id: UUID
    change_log: List[ChangeLog]


class TaskCreateIn(StrictBaseModel):
    asset_id: UUID
    change_log: List[ChangeLog]


class TaskUpdateIn(StrictBaseModel):
    change_log: List[ChangeLog]


class TaskResponse(Response):
    data: Task


class TasksResponse(Response):
    data: List[Task]


class PaginatedTasksResponse(TasksResponse):
    links: PaginationLinks
    meta: PaginationMeta
