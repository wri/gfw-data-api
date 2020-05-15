from typing import List
from uuid import UUID

from asyncpg import UniqueViolationError
from fastapi import HTTPException

from . import update_data
from ..models.orm.asset import Asset as ORMAsset


async def get_assets(dataset: str, version: str) -> List[ORMAsset]:
    rows: List[ORMAsset] = await ORMAsset.query.where(
        ORMAsset.dataset == dataset
    ).where(ORMAsset.version == version).gino.all()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Version with name {dataset}/{version} does not exist",
        )

    return rows


async def get_all_assets() -> List[ORMAsset]:
    return await ORMAsset.query.gino.all()


async def get_assets_by_type(asset_type: str) -> List[ORMAsset]:
    return await ORMAsset.query.where(ORMAsset.asset_type == asset_type).gino.all()


async def get_asset(asset_id: UUID) -> ORMAsset:
    row: ORMAsset = await ORMAsset.get([asset_id])
    if row is None:
        raise HTTPException(
            status_code=404, detail=f"Could not find requested asset {asset_id}",
        )
    return row


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

    return new_asset


async def update_asset(asset_id: UUID, **data) -> ORMAsset:
    row: ORMAsset = await get_asset(asset_id)
    return await update_data(row, data)


async def delete_asset(asset_id: UUID) -> ORMAsset:
    row: ORMAsset = await get_asset(asset_id)
    await ORMAsset.delete.where(ORMAsset.asset_id == asset_id).gino.status()

    return row
