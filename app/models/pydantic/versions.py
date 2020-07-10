from typing import List, Optional, Tuple

from pydantic import BaseModel

from ..enum.sources import SourceType
from ..enum.versions import VersionStatus
from .base import Base
from .change_log import ChangeLog
from .creation_options import CreationOptions, SourceCreationOptions
from .metadata import VersionMetadata
from .responses import Response


class Version(Base):
    dataset: str
    version: str
    is_latest: bool = False
    is_mutable: bool = False
    metadata: VersionMetadata
    status: VersionStatus = VersionStatus.pending

    assets: List[Tuple[str, str]] = list()


class VersionCreateIn(BaseModel):
    metadata: VersionMetadata
    creation_options: SourceCreationOptions


class VersionUpdateIn(BaseModel):
    is_latest: Optional[bool]
    metadata: Optional[VersionMetadata]
    # creation_options: Optional[SourceCreationOptions]


class VersionResponse(Response):
    data: Version
