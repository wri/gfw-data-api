from typing import List
from uuid import UUID

from pydantic import BaseModel

from .base import Base
from .change_log import ChangeLog
from .responses import Response


class Task(Base):
    task_id: UUID
    asset_id: UUID
    change_log: List[ChangeLog]


class TaskUpdateIn(BaseModel):
    change_log: List[ChangeLog]


class TaskResponse(Response):
    data: Task


class TasksResponse(Response):
    data: List[Task]
