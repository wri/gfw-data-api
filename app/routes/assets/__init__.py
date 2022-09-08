from typing import List

from fastapi.logger import logger

from ...models.orm.assets import Asset as ORMAsset
from ...models.pydantic.assets import (
    Asset,
    AssetResponse,
    AssetsResponse,
    PaginatedAssetsResponse,
)
from ...models.pydantic.metadata import asset_metadata_factory
from ...models.pydantic.responses import PaginationLinks, PaginationMeta


async def asset_response(asset_orm: ORMAsset) -> AssetResponse:
    """Serialize ORM response."""

    data: Asset = await _serialized_asset(asset_orm)
    return AssetResponse(data=data)


async def assets_response(assets_orm: List[ORMAsset]) -> AssetsResponse:
    """Serialize ORM response."""
    data = [await _serialized_asset(asset_orm) for asset_orm in assets_orm]
    return AssetsResponse(data=data)


async def paginated_assets_response(
    assets_orm: List[ORMAsset], links: PaginationLinks, meta: PaginationMeta
) -> PaginatedAssetsResponse:
    """Serialize ORM response."""
    data = [await _serialized_asset(asset_orm) for asset_orm in assets_orm]
    return PaginatedAssetsResponse(data=data, links=links, meta=meta)


async def _serialized_asset(asset_orm: ORMAsset) -> Asset:

    data: Asset = Asset.from_orm(asset_orm)
    data.metadata = asset_metadata_factory(asset_orm.asset_type, asset_orm.metadata)

    logger.debug(f"Metadata: {data.metadata.dict(by_alias=True)}")
    return data
