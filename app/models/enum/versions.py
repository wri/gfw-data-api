from sqlalchemy import Enum


class VersionStatus(str, Enum):
    saved = "saved"
    pending = "pending"
    failed = "failed"
