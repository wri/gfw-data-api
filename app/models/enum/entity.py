from enum import StrEnum


class EntityType(StrEnum):
    saved = "dataset"
    pending = "version"
    failed = "asset"
