from typing import Any, Dict, List
from uuid import UUID

from asyncpg import UniqueViolationError
from fastapi import HTTPException

from ..models.orm.assets import Asset as ORMAsset
from ..models.orm.datasets import Dataset as ORMDataset
from ..models.orm.versions import Version as ORMVersion
from ..models.pydantic.creation_options import (
    CreationOptions,
    StaticVectorTileCacheCreationOptions,
    TableDrivers,
    TableSourceCreationOptions,
    VectorDrivers,
    VectorSourceCreationOptions,
)
from . import datasets, update_all_metadata, update_data, update_metadata, versions


async def get_assets(dataset: str, version: str) -> List[ORMAsset]:
    rows: List[ORMAsset] = await ORMAsset.query.where(
        ORMAsset.dataset == dataset
    ).where(ORMAsset.version == version).gino.all()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Version with name {dataset}.{version} does not exist",
        )
    d: ORMDataset = await datasets.get_dataset(dataset)
    v: ORMVersion = await versions.get_version(dataset, version)

    v = update_metadata(v, d)

    return update_all_metadata(rows, v)


async def get_all_assets() -> List[ORMAsset]:
    assets = await ORMAsset.query.gino.all()

    return await _update_all_asset_metadata(assets)


async def get_assets_by_type(asset_type: str) -> List[ORMAsset]:
    assets = await ORMAsset.query.where(ORMAsset.asset_type == asset_type).gino.all()
    return await _update_all_asset_metadata(assets)


async def get_asset(asset_id: UUID) -> ORMAsset:
    row: ORMAsset = await ORMAsset.get([asset_id])
    if row is None:
        raise HTTPException(
            status_code=404, detail=f"Could not find requested asset {asset_id}",
        )
    dataset: ORMDataset = await datasets.get_dataset(row.dataset)
    version: ORMVersion = await versions.get_version(row.dataset, row.version)
    version = update_metadata(version, dataset)

    return update_metadata(row, version)


async def create_asset(dataset, version, **data) -> ORMAsset:

    data = _validate_creation_options(**data)

    try:
        new_asset: ORMAsset = await ORMAsset.create(
            dataset=dataset, version=version, **data
        )
    except UniqueViolationError:
        raise HTTPException(
            status_code=400,
            detail="A similar Asset already exist." "Asset uri must be unique.",
        )

    d: ORMDataset = await datasets.get_dataset(dataset)
    v: ORMVersion = await versions.get_version(dataset, version)
    v = update_metadata(v, d)

    return update_metadata(new_asset, v)


async def update_asset(asset_id: UUID, **data) -> ORMAsset:

    data = _validate_creation_options(**data)

    row: ORMAsset = await get_asset(asset_id)
    row = await update_data(row, data)

    dataset: ORMDataset = await datasets.get_dataset(row.dataset)
    version: ORMVersion = await versions.get_version(row.dataset, row.version)
    version = update_metadata(version, dataset)

    return update_metadata(row, version)


async def delete_asset(asset_id: UUID) -> ORMAsset:
    row: ORMAsset = await get_asset(asset_id)
    await ORMAsset.delete.where(ORMAsset.asset_id == asset_id).gino.status()

    dataset: ORMDataset = await datasets.get_dataset(row.dataset)
    version: ORMVersion = await versions.get_version(row.dataset, row.version)
    version = update_metadata(version, dataset)

    return update_metadata(row, version)


async def _update_all_asset_metadata(assets):
    new_rows = list()
    for row in assets:
        dataset: ORMDataset = await datasets.get_dataset(row.dataset)
        version: ORMVersion = await versions.get_version(row.dataset, row.version)
        version = update_metadata(version, dataset)
        update_metadata(row, version)
        new_rows.append(row)

    return new_rows


def _validate_creation_options(**data) -> Dict[str, Any]:
    """Validate if submitted creation options match asset type."""

    if "creation_options" in data.keys() and "asset_type" in data.keys():
        asset_type = data["asset_type"]
        creation_options = data["creation_options"]

        co_model: CreationOptions = _creation_option_factory(
            asset_type, creation_options
        )

        data["creation_options"] = co_model.dict()

    return data


def _creation_option_factory(asset_type, creation_options) -> CreationOptions:
    """Create creation options pydantic model based on asset type."""

    driver = creation_options.get("src_driver", None)
    table_drivers: List[str] = [t.value for t in TableDrivers]
    vector_drivers: List[str] = [v.value for v in VectorDrivers]

    if asset_type == "Database table" and driver in table_drivers:
        model = TableSourceCreationOptions(**creation_options)

    elif asset_type == "Database table" and driver in vector_drivers:
        model = VectorSourceCreationOptions(**creation_options)

    elif asset_type == "Vector tile cache":
        model = StaticVectorTileCacheCreationOptions(**creation_options)

    else:
        raise HTTPException(
            status_code=501,
            detail=f"Creation options validation for {asset_type} not implemented",
        )

    return model
