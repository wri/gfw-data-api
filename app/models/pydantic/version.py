from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from .base import Base
from .change_log import ChangeLog
from .metadata import FieldMetadata, VersionMetadata
from .source import SourceType


class Version(Base):
    dataset: str
    version: str
    is_latest: bool = False
    is_mutable: bool = False
    source_type: SourceType
    source_uri: Optional[List[str]] = None
    metadata: VersionMetadata
    status: str

    # Tablular/ Vector data only

    has_geostore: Optional[bool]

    assets: List[Tuple[str, str]] = list()
    change_log: List[ChangeLog]


class VersionCreateIn(BaseModel):
    is_latest: bool = False
    source_type: SourceType
    source_uri: Optional[List[str]]
    metadata: VersionMetadata


class VersionUpdateIn(BaseModel):
    is_latest: Optional[bool]
    source_type: Optional[SourceType]
    source_uri: Optional[List[str]]
    metadata: Optional[VersionMetadata]
