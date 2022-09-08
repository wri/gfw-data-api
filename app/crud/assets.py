from typing import Any, Dict, List, Optional
from uuid import UUID

from asyncpg import UniqueViolationError
from fastapi.encoders import jsonable_encoder
from sqlalchemy import func

from ..errors import RecordAlreadyExistsError, RecordNotFoundError
from ..models.orm.assets import Asset as ORMAsset
from ..models.orm.versions import Version as ORMVersion
from ..models.pydantic.creation_options import CreationOptions, creation_option_factory
from . import update_data, versions
from .metadata import update_all_metadata, update_metadata


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


async def get_assets_by_type(asset_type: str) -> List[ORMAsset]:
    assets = await ORMAsset.query.where(ORMAsset.asset_type == asset_type).gino.all()
    return await _update_all_asset_metadata(assets)


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
) -> List[ORMAsset]:

    query = await _build_filtered_query(
        asset_types, asset_uri, dataset, is_default, is_latest, version
    )
    assets = await query.gino.load(ORMAsset).all()

    return await _update_all_asset_metadata(assets)


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
        return await _update_all_asset_metadata(assets)

    return paginated_assets


async def get_asset(asset_id: UUID) -> ORMAsset:
    row: ORMAsset = await ORMAsset.get([asset_id])
    if row is None:
        raise RecordNotFoundError(f"Could not find requested asset {asset_id}")

    version: ORMVersion = await versions.get_version(row.dataset, row.version)

    return update_metadata(row, version)


async def get_default_asset(dataset: str, version: str) -> ORMAsset:
    row: ORMAsset = (
        await ORMAsset.query.where(ORMAsset.dataset == dataset)
        .where(ORMAsset.version == version)
        .where(ORMAsset.is_default == True)  # noqa: E712
        .gino.first()
    )
    if row is None:
        raise RecordNotFoundError(
            f"Could not find default asset for {dataset}.{version}"
        )

    v: ORMVersion = await versions.get_version(row.dataset, row.version)

    return update_metadata(row, v)


async def create_asset(dataset, version, **data) -> ORMAsset:
    v: ORMVersion = await versions.get_version(dataset, version)

    # default to version.is_downloadable if not set
    if data.get("is_downloadable") is None:
        data["is_downloadable"] = v.is_downloadable

    data = _validate_creation_options(**data)
    jsonable_data = jsonable_encoder(data)

    try:
        new_asset: ORMAsset = await ORMAsset.create(
            dataset=dataset, version=version, **jsonable_data
        )
    except UniqueViolationError:
        raise RecordAlreadyExistsError(
            f"Cannot create asset of type {data['asset_type']}. "
            f"Asset uri must be unique. An asset with uri {data['asset_uri']} already exists"
        )

    return update_metadata(new_asset, v)


async def update_asset(asset_id: UUID, **data) -> ORMAsset:
    data = _validate_creation_options(**data)
    jsonable_data = jsonable_encoder(data)

    row: ORMAsset = await get_asset(asset_id)
    row = await update_data(row, jsonable_data)

    version: ORMVersion = await versions.get_version(row.dataset, row.version)

    return update_metadata(row, version)


async def delete_asset(asset_id: UUID) -> ORMAsset:
    row: ORMAsset = await get_asset(asset_id)
    await ORMAsset.delete.where(ORMAsset.asset_id == asset_id).gino.status()

    version: ORMVersion = await versions.get_version(row.dataset, row.version)

    return update_metadata(row, version)


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
