from typing import List, Optional
from uuid import UUID

from pydantic import Field

from ..enum.assets import AssetStatus, AssetType
from .base import BaseRecord, StrictBaseModel
from .creation_options import (
    CreationOptions,
    OtherCreationOptions,
    SourceCreationOptions,
)
from .metadata import AssetMetadata, VersionMetadata
from .responses import Response


class Asset(BaseRecord):
    asset_id: UUID
    dataset: str
    version: str
    asset_type: AssetType
    asset_uri: str
    status: AssetStatus = AssetStatus.pending
    is_managed: bool
    is_downloadable: bool
    metadata: AssetMetadata


class RevisionHistory(StrictBaseModel):
    dataset: str
    version: str
    metadata: VersionMetadata
    creation_options: SourceCreationOptions


class AssetCreateIn(StrictBaseModel):
    asset_type: AssetType
    asset_uri: Optional[str]
    is_managed: bool = True
    is_downloadable: Optional[bool] = Field(
        None,
        description="Flag to specify if assets associated with version can be downloaded."
        "If not set, value will default to settings of underlying version.",
    )
    creation_options: OtherCreationOptions
    metadata: Optional[AssetMetadata]


class AssetUpdateIn(StrictBaseModel):
    is_downloadable: Optional[bool] = Field(
        None,
        description="Flag to specify if assets associated with version can be downloaded."
        "If not set, value will default to settings of underlying version.",
    )
    metadata: Optional[AssetMetadata]


class AssetTaskCreate(StrictBaseModel):
    asset_type: AssetType
    dataset: str
    version: str
    asset_uri: Optional[str]
    is_managed: bool = True
    is_default: bool = False
    is_downloadable: Optional[bool] = None
    creation_options: CreationOptions  # should this also be OtherCreationOptions?
    metadata: Optional[AssetMetadata]
    revision_history: List[RevisionHistory]
    source_version: str


class AssetResponse(Response):
    data: Asset


class AssetsResponse(Response):
    data: List[Asset]
