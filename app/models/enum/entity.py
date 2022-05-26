from enum import Enum


class EntityType(str, Enum):
    saved = "dataset"
    pending = "version"
    failed = "asset"
