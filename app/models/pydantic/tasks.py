from typing import List, Optional, Tuple

from pydantic import BaseModel

from .base import Base
from .change_log import ChangeLog


class Task(Base):
    task_id: str
    asset_id: str
    change_log: List[ChangeLog]


class TaskUpdateIn(BaseModel):
    # asset_id: str
    change_log: List[ChangeLog]
