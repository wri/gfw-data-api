from enum import Enum


class ChangeLogStatusTaskIn(str, Enum):
    success = "success"
    failed = "failed"


class ChangeLogStatus(str, Enum):
    success = "success"
    failed = "failed"
    pending = "pending"
