from typing import Any, Dict, List, Optional
from uuid import UUID

from asyncpg import UniqueViolationError
from fastapi.encoders import jsonable_encoder
from sqlalchemy.sql import and_

from app.crud.metadata import (
    create_asset_metadata,
    get_asset_metadata,
    update_asset_metadata,
)
from sqlalchemy import func
from async_lru import alru_cache

from ..errors import RecordAlreadyExistsError, RecordNotFoundError
from ..models.enum.assets import AssetType
from ..models.orm.asset_metadata import AssetMetadata as ORMAssetMetadata
from ..models.orm.assets import Asset as ORMAsset
from ..models.orm.versions import Version as ORMVersion
from ..models.pydantic.creation_options import CreationOptions, creation_option_factory
from ..models.pydantic.asset_metadata import RasterTileSetMetadataOut
from . import update_data, versions


async def get_assets(dataset: str, version: str) -> List[ORMAsset]:
    rows: List[ORMAsset] = (
        await ORMAsset.query.where(ORMAsset.dataset == dataset)
        .where(ORMAsset.version == version)
        .order_by(ORMAsset.created_on)
        .gino.all()
    )
    if not rows:
        raise RecordNotFoundError(
            f"No assets for version with name {dataset}.{version} found"
        )

    return rows


async def get_assets_by_type(asset_type: str) -> List[ORMAsset]:
    assets = await ORMAsset.query.where(ORMAsset.asset_type == asset_type).gino.all()

    return assets


async def _build_filtered_query(
    asset_types, asset_uri, dataset, is_default, is_latest, version
):
    if is_latest is not None:
        VersionAliased = ORMVersion.alias()
        query = (
            ORMAsset.join(VersionAliased)
            .select()
            .where(VersionAliased.is_latest == is_latest)
        )
    else:
        query = ORMAsset.query
    if dataset is not None:
        query = query.where(ORMAsset.dataset == dataset)
    if version is not None:
        query = query.where(ORMAsset.version == version)
    if asset_types:
        query = query.where(ORMAsset.asset_type.in_(asset_types))
    if asset_uri is not None:
        query = query.where(ORMAsset.asset_uri == asset_uri)
    if is_default is not None:
        query = query.where(ORMAsset.is_default == is_default)

    query = query.order_by(ORMAsset.created_on)
    return query


async def get_assets_by_filter(
    dataset: Optional[str] = None,
    version: Optional[str] = None,
    asset_types: Optional[List[str]] = None,
    asset_uri: Optional[str] = None,
    is_latest: Optional[bool] = None,
    is_default: Optional[bool] = None,
    include_metadata: Optional[bool] = True,
) -> List[ORMAsset]:

    query = await _build_filtered_query(
        asset_types, asset_uri, dataset, is_default, is_latest, version
    )
    assets = await query.gino.load(ORMAsset).all()

    if include_metadata:
        for asset in assets:
            try:
                asset.metadata = await get_asset_metadata(asset.asset_id)
            except RecordNotFoundError:
                asset.metadata = None

    return assets


async def count_filtered_assets_fn(
    dataset: Optional[str] = None,
    version: Optional[str] = None,
    asset_types: Optional[List[str]] = None,
    asset_uri: Optional[str] = None,
    is_latest: Optional[bool] = None,
    is_default: Optional[bool] = None,
) -> func:
    """Returns a function that counts all filtered assets.

    This higher-order function is designed to be used with the
    pagination utility. It relies on the closure to set all the
    necessary filtering so that pagination doesn't need to know any more
    than the essentials for getting a record count.
    """
    query = await _build_filtered_query(
        asset_types, asset_uri, dataset, is_default, is_latest, version
    )

    async def count_assets() -> int:
        return await func.count().select().select_from(query.alias()).gino.scalar()

    return count_assets


async def get_filtered_assets_fn(
    dataset: Optional[str] = None,
    version: Optional[str] = None,
    asset_types: Optional[List[str]] = None,
    asset_uri: Optional[str] = None,
    is_latest: Optional[bool] = None,
    is_default: Optional[bool] = None,
    include_metadata: Optional[bool] = True,
) -> func:
    """Returns a function that retrieves all filtered assets.

    This higher-order function is designed to be used with the
    pagination utility. It relies on the closure to set all the
    necessary filtering so that pagination doesn't need to know any more
    than the essentials for getting asset records.
    """
    query = await _build_filtered_query(
        asset_types, asset_uri, dataset, is_default, is_latest, version
    )

    async def paginated_assets(size: int = None, offset: int = 0) -> List[ORMAsset]:
        assets = await query.limit(size).offset(offset).gino.load(ORMAsset).all()
        if include_metadata:
            for asset in assets:
                try:
                    asset.metadata = await get_asset_metadata(asset.asset_id)
                except RecordNotFoundError:
                    asset.metadata = None

        return assets

    return paginated_assets


async def get_asset(asset_id: UUID) -> ORMAsset:
    asset = await ORMAsset.get([asset_id])

    if asset is None:
        raise RecordNotFoundError(f"Could not find requested asset {asset_id}")

    try:
        metadata: ORMAssetMetadata = await get_asset_metadata(asset_id)
        asset.metadata = metadata
    except RecordNotFoundError:
        asset.metadata = None

    return asset


async def get_default_asset(dataset: str, version: str) -> ORMAsset:
    asset: ORMAsset = (
        await ORMAsset.query.where(ORMAsset.dataset == dataset)
        .where(ORMAsset.version == version)
        .where(ORMAsset.is_default == True)  # noqa: E712
        .gino.first()
    )
    if asset is None:
        raise RecordNotFoundError(
            f"Could not find default asset for {dataset}.{version}"
        )

    try:
        metadata: ORMAssetMetadata = await get_asset_metadata(asset.asset_id)
        asset.metadata = metadata
    except RecordNotFoundError:
        asset.metadata = None

    return asset


async def create_asset(dataset, version, **data) -> ORMAsset:
    v: ORMVersion = await versions.get_version(dataset, version)

    # default to version.is_downloadable if not set
    if data.get("is_downloadable") is None:
        data["is_downloadable"] = v.is_downloadable

    metadata_data = data.pop("metadata", None)
    if metadata_data:
        metadata_data = jsonable_encoder(metadata_data, exclude_unset=True)
    data = _validate_creation_options(**data)
    jsonable_data = jsonable_encoder(data)
    try:
        new_asset: ORMAsset = await ORMAsset.create(
            dataset=dataset, version=version, **jsonable_data
        )
        new_asset.metadata = None
    except UniqueViolationError:
        raise RecordAlreadyExistsError(
            f"Cannot create asset of type {data['asset_type']}. "
            f"Asset uri must be unique. An asset with uri {data['asset_uri']} already exists"
        )

    if metadata_data:
        metadata: ORMAssetMetadata = await create_asset_metadata(
            new_asset.asset_id, **metadata_data
        )
        new_asset.metadata = metadata

    return new_asset


async def update_asset(asset_id: UUID, **data) -> ORMAsset:
    metadata_data = data.pop("metadata", None)
    data = _validate_creation_options(**data)
    jsonable_data = jsonable_encoder(data)

    asset: ORMAsset = await get_asset(asset_id)
    asset = await update_data(asset, jsonable_data)

    if metadata_data:
        try:
            metadata = await update_asset_metadata(asset_id, **metadata_data)
        except RecordNotFoundError:
            metadata = await create_asset_metadata(asset_id, **metadata_data)
        asset.metadata = metadata

    return asset


async def delete_asset(asset_id: UUID) -> ORMAsset:
    asset: ORMAsset = await get_asset(asset_id)
    await ORMAsset.delete.where(ORMAsset.asset_id == asset_id).gino.status()

    return asset


def _validate_creation_options(**data) -> Dict[str, Any]:
    """Validate if submitted creation options match asset type."""

    if "creation_options" in data.keys() and "asset_type" in data.keys():
        asset_type = data["asset_type"]
        creation_options = data["creation_options"]
        co_model: CreationOptions = creation_option_factory(
            asset_type, creation_options
        )

        data["creation_options"] = co_model.dict(by_alias=True)

    return data
