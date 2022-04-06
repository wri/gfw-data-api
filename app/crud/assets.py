from typing import Any, Dict, List, Optional
from uuid import UUID

from asyncpg import UniqueViolationError
from fastapi.encoders import jsonable_encoder

from app.crud.metadata import create_asset_metadata, get_asset_metadata

from ..errors import RecordAlreadyExistsError, RecordNotFoundError
from ..models.orm.assets import Asset as ORMAsset
from ..models.orm.versions import Version as ORMVersion
from ..models.orm.asset_metadata import AssetMetadata as ORMAssetMetadata
from ..models.pydantic.creation_options import CreationOptions, creation_option_factory
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

    v: ORMVersion = await versions.get_version(dataset, version)

    return update_all_metadata(rows, v)


async def get_all_assets() -> List[ORMAsset]:
    assets = await ORMAsset.query.gino.all()

    return assets


async def get_assets_by_type(asset_type: str) -> List[ORMAsset]:
    assets = await ORMAsset.query.where(ORMAsset.asset_type == asset_type).gino.all()

    return assets


async def get_assets_by_filter(
    dataset: Optional[str] = None,
    version: Optional[str] = None,
    asset_types: Optional[List[str]] = None,
    asset_uri: Optional[str] = None,
    is_latest: Optional[bool] = None,
    is_default: Optional[bool] = None,
) -> List[ORMAsset]:
    query = ORMAsset.query
    if dataset is not None:
        query = query.where(ORMAsset.dataset == dataset)
    if version is not None:
        query = query.where(ORMAsset.version == version)
    if asset_types:
        query = query.where(ORMAsset.asset_type.in_(asset_types))
    if asset_uri is not None:
        query = query.where(ORMAsset.asset_uri == asset_uri)
    if is_latest is not None:
        query = query.where(ORMAsset.is_latest == is_latest)
    if is_default is not None:
        query = query.where(ORMAsset.is_default == is_default)

    query = query.order_by(ORMAsset.created_on)
    assets = await query.gino.all()

    return assets


async def get_asset(asset_id: UUID) -> ORMAsset:
    asset = await ORMAsset.get([asset_id])

    if asset is None:
        raise RecordNotFoundError(f"Could not find requested asset {asset_id}")

    try:
        metadata: ORMAssetMetadata = await get_asset_metadata(asset_id)
        asset.metadata = metadata
    except RecordNotFoundError:
        pass

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

    return asset


async def create_asset(dataset, version, **data) -> ORMAsset:
    v: ORMVersion = await versions.get_version(dataset, version)

    # default to version.is_downloadable if not set
    if data.get("is_downloadable") is None:
        data["is_downloadable"] = v.is_downloadable

    data = _validate_creation_options(**data)
    jsonable_data = jsonable_encoder(data)
    metadata_data = jsonable_data.pop("metadata", None)
    try:
        new_asset: ORMAsset = await ORMAsset.create(
            dataset=dataset, version=version, **jsonable_data
        )
    except UniqueViolationError:
        raise RecordAlreadyExistsError(
            f"Cannot create asset of type {data['asset_type']}. "
            f"Asset uri must be unique. An asset with uri {data['asset_uri']} already exists"
        )

    if metadata_data:
        metadata: ORMAssetMetadata = create_asset_metadata(
            new_asset.asset_id, **metadata_data
        )
        new_asset.metadata = metadata

    return new_asset


async def update_asset(asset_id: UUID, **data) -> ORMAsset:
    data = _validate_creation_options(**data)
    jsonable_data = jsonable_encoder(data)

    asset: ORMAsset = await get_asset(asset_id)
    asset = await update_data(asset, jsonable_data)

    return asset


async def delete_asset(asset_id: UUID) -> ORMAsset:
    asset: ORMAsset = await get_asset(asset_id)
    await ORMAsset.delete.where(ORMAsset.asset_id == asset_id).gino.status()

    return asset


async def _update_all_asset_metadata(assets) -> List[ORMAsset]:
    new_rows: List[ORMAsset] = list()
    for row in assets:
        version: ORMVersion = await versions.get_version(row.dataset, row.version)
        new_row = update_metadata(row, version)
        new_rows.append(new_row)

    return new_rows


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
