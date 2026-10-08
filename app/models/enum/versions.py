from enum import Enum


class VersionStatus(str, Enum):
    saved = "saved"
    pending = "pending"
    failed = "failed"
