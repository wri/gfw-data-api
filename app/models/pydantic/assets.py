from typing import Any, List, Literal, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field

from ..enum.assets import AssetStatus, AssetType
from .base import BaseRecord, StrictBaseModel
from .creation_options import (
    CreationOptions,
    DynamicVectorTileCacheCreationOptions,
    RasterTileCacheCreationOptions,
    RasterTileSetAssetCreationOptions,
    StaticVectorFileCreationOptions,
    StaticVectorTileCacheCreationOptions,
    TableAssetCreationOptions,
)
from .metadata import (
    AssetMetadata,
    DatabaseTableMetadata,
    DynamicVectorTileCacheMetadata,
    RasterTileCacheMetadata,
    RasterTileSetMetadata,
    StaticVectorTileCacheMetadata,
    VectorFileMetadata,
)
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


class BaseAssetCreateIn(StrictBaseModel):
    asset_type: str
    creation_options: Any
    metadata: Any
    asset_uri: Optional[str]
    is_managed: bool = True


class RasterTileSetAssetCreateIn(BaseAssetCreateIn):
    asset_type: Literal[AssetType.raster_tile_set]  # type: ignore
    creation_options: RasterTileSetAssetCreationOptions
    metadata: Optional[RasterTileSetMetadata] = None


class RasterTileCacheAssetCreateIn(BaseAssetCreateIn):
    asset_type: Literal[AssetType.raster_tile_cache]  # type: ignore
    creation_options: RasterTileCacheCreationOptions
    metadata: Optional[RasterTileCacheMetadata] = None


class StaticVectorTileCacheAssetCreateIn(BaseAssetCreateIn):
    asset_type: Literal[AssetType.static_vector_tile_cache]  # type: ignore
    creation_options: StaticVectorTileCacheCreationOptions
    metadata: Optional[StaticVectorTileCacheMetadata] = None


class StaticVectorFileAssetCreateIn(BaseAssetCreateIn):
    asset_type: Literal[AssetType.shapefile]  # type: ignore # FIXME: should be any static vector file
    # Union[Literal[AssetType.shapefile], Literal[AssetType.geopackage], Literal[AssetType.ndjson]]
    creation_options: StaticVectorFileCreationOptions
    metadata: Optional[VectorFileMetadata] = None


class DynamicVectorTileCacheAssetCreateIn(BaseAssetCreateIn):
    asset_type: Literal[AssetType.dynamic_vector_tile_cache]  # type: ignore
    creation_options: DynamicVectorTileCacheCreationOptions
    metadata: Optional[DynamicVectorTileCacheMetadata] = None


class TableAssetCreateIn(BaseAssetCreateIn):
    asset_type: Literal[AssetType.csv]  # type: ignore
    creation_options: TableAssetCreationOptions
    metadata: Optional[DatabaseTableMetadata] = None


# AssetCreateIn = Annotated[Union[RasterTileSetAssetCreateIn, RasterTileCacheAssetCreateIn, StaticVectorTileCacheAssetCreateIn], Field(discriminator='asset_type')]


class AssetCreateIn(BaseModel):
    __root__: Union[
        RasterTileSetAssetCreateIn,
        RasterTileCacheAssetCreateIn,
        StaticVectorTileCacheAssetCreateIn,
        StaticVectorFileAssetCreateIn,
        DynamicVectorTileCacheAssetCreateIn,
        TableAssetCreateIn,
    ] = Field(..., discriminator="asset_type")


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
