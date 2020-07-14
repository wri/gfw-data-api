from typing import List, Optional, Tuple

from pydantic import BaseModel

from ..enum.versions import VersionStatus
from .base import Base
from .creation_options import SourceCreationOptions
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


class VersionAppendIn(BaseModel):
    source_uri: List[str]


class VersionResponse(Response):
    data: Version
