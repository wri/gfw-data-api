from typing import List
from uuid import UUID

from asyncpg import UniqueViolationError
from fastapi import HTTPException

from ..models.orm.assets import Asset as ORMAsset
from ..models.orm.datasets import Dataset as ORMDataset
from ..models.orm.versions import Version as ORMVersion
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
    try:
        new_asset: ORMAsset = await ORMAsset.create(
            dataset=dataset, version=version, **data
        )
    except UniqueViolationError:
        raise HTTPException(
            status_code=400,
            detail="A similar Asset already exist."
            "Dataset versions can only have one instance of asset type."
            "Asset uri must be unique.",
        )

    d: ORMDataset = await datasets.get_dataset(dataset)
    v: ORMVersion = await versions.get_version(dataset, version)
    v = update_metadata(v, d)

    return update_metadata(new_asset, v)


async def update_asset(asset_id: UUID, **data) -> ORMAsset:
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
