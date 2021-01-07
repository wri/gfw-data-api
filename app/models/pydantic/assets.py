from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel

from ..enum.assets import AssetStatus, AssetType
from .base import BaseRecord, StrictBaseModel
from .creation_options import CreationOptions, OtherCreationOptions
from .metadata import AssetMetadata
from .responses import Response


class Asset(BaseRecord):
    asset_id: UUID
    dataset: str
    version: str
    asset_type: AssetType
    asset_uri: str
    status: AssetStatus = AssetStatus.pending
    is_managed: bool
    metadata: AssetMetadata


class AssetCreateIn(StrictBaseModel):
    asset_type: AssetType
    asset_uri: Optional[str]
    is_managed: bool = True
    creation_options: OtherCreationOptions
    metadata: Optional[AssetMetadata]

    class Config:
        extra = "forbid"

class AssetUpdateIn(StrictBaseModel):
    metadata: AssetMetadata


class AssetTaskCreate(StrictBaseModel):
    asset_type: AssetType
    dataset: str
    version: str
    asset_uri: Optional[str]
    is_managed: bool = True
    is_default: bool = False
    creation_options: CreationOptions  # should this also be OtherCreationOptions?
    metadata: Optional[AssetMetadata]


class AssetResponse(Response):
    data: Asset


class AssetsResponse(Response):
    data: List[Asset]
