from typing import List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field

from ..enum.assets import AssetStatus, AssetType
from .asset_metadata import AssetMetadata, AssetMetadataOut
from .base import BaseRecord, StrictBaseModel
from .creation_options import CreationOptions, OtherCreationOptions
from .responses import PaginationLinks, PaginationMeta, Response


class Asset(BaseRecord):
    asset_id: UUID
    dataset: str
    version: str
    asset_type: AssetType
    asset_uri: str
    status: AssetStatus = AssetStatus.pending
    is_managed: bool
    is_downloadable: bool
    metadata: Optional[Union[AssetMetadataOut, BaseModel]]


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


class AssetResponse(Response):
    data: Asset


class AssetsResponse(Response):
    data: List[Asset]


class PaginatedAssetsResponse(AssetsResponse):
    links: PaginationLinks
    meta: PaginationMeta
