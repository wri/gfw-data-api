from enum import StrEnum


class VersionStatus(StrEnum):
    saved = "saved"
    pending = "pending"
    failed = "failed"
