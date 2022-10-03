from typing import List

from fastapi.logger import logger

from ...models.orm.assets import Asset as ORMAsset
from ...models.pydantic.asset_metadata import asset_metadata_factory
from ...models.pydantic.assets import (
    Asset,
    AssetResponse,
    AssetsResponse,
    PaginatedAssetsResponse,
)
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
    metadata = asset_metadata_factory(asset_orm)

    if hasattr(asset_orm, "metadata"):
        delattr(asset_orm, "metadata")
    data: Asset = Asset.from_orm(asset_orm)
    data.metadata = metadata

    logger.debug(f"Metadata: {data.metadata.dict(by_alias=True)}")
    return data
