from enum import StrEnum


class ChangeLogStatusTaskIn(StrEnum):
    success = "success"
    failed = "failed"


class ChangeLogStatus(StrEnum):
    success = "success"
    failed = "failed"
    pending = "pending"
