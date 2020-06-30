from enum import Enum


class ChangeLogStatus(str, Enum):
    success = "success"
    pending = "pending"
    failed = "failed"
