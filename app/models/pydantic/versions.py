from typing import Any, Dict, List, Optional, Tuple

from pydantic import Field

from ..enum.versions import VersionStatus
from .base import BaseRecord, StrictBaseModel
from .creation_options import SourceCreationOptions
from .metadata import VersionMetadata
from .responses import Response


class Version(BaseRecord):
    dataset: str
    version: str
    is_latest: bool = False
    is_mutable: bool = False
    metadata: VersionMetadata
    status: VersionStatus = VersionStatus.pending

    assets: List[Tuple[str, str]] = list()


class VersionCreateIn(StrictBaseModel):
    is_downloadable: Optional[bool] = Field(
        None,
        description="Flag to specify if assets associated with version can be downloaded."
        "If not set, value will default to settings of underlying dataset",
    )
    metadata: Optional[VersionMetadata] = Field(
        None,
        description="Version metadata. Version will inherit metadata from dataset. "
        "You will only need to add fields which you want to add or overwrite.",
    )
    creation_options: SourceCreationOptions = Field(
        ...,
        description="Creation option to specify how default asset for version should be created.",
    )
    history: Dict[str, Any] = Field(
        [],
        description="Revision history of the version.",
    )


class VersionUpdateIn(StrictBaseModel):
    is_downloadable: Optional[bool] = Field(
        None,
        description="Flag to specify if assets associated with version can be downloaded."
        "If not set, value will default to settings of underlying dataset",
    )
    is_latest: Optional[bool] = Field(
        None,
        description="Indicate if the current version should be tagged `latest`. "
        "This will cause redirects from {dataset}/latest to {dataset}/{current_version}."
        "When tagging a version to `latest` any other version currently tagged `latest` will be untagged.",
    )
    metadata: Optional[VersionMetadata] = Field(
        None,
        description="Version metadata. Version will inherit metadata from dataset. "
        "You will only need to add fields which you want to add or overwrite.",
    )


class VersionHistory(StrictBaseModel):
    dataset: str
    version: str
    metadata: VersionMetadata
    creation_options: SourceCreationOptions


class VersionAppendIn(StrictBaseModel):
    source_uri: List[str]


class VersionResponse(Response):
    data: Version
