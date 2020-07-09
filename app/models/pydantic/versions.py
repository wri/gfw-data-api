from typing import List, Optional, Tuple

from pydantic import BaseModel

from ..enum.sources import SourceType
from ..enum.versions import VersionStatus
from .base import Base
from .change_log import ChangeLog
from .creation_options import CreationOptions
from .metadata import VersionMetadata
from .responses import Response


class Version(Base):
    dataset: str
    version: str
    is_latest: bool = False
    is_mutable: bool = False
    source_type: SourceType
    source_uri: Optional[List[str]] = None
    metadata: VersionMetadata
    status: VersionStatus = VersionStatus.pending
    creation_options: CreationOptions

    # Tablular/ Vector data only

    has_geostore: Optional[bool]

    assets: List[Tuple[str, str]] = list()
    change_log: List[ChangeLog]


class VersionCreateIn(BaseModel):
    is_latest: bool = False
    source_type: SourceType
    source_uri: List[str]
    metadata: VersionMetadata
    creation_options: CreationOptions


class VersionUpdateIn(BaseModel):
    is_latest: Optional[bool]
    source_type: Optional[SourceType]
    source_uri: Optional[List[str]]
    metadata: Optional[VersionMetadata]
    creation_options: Optional[CreationOptions]


class VersionResponse(Response):
    data: Version
