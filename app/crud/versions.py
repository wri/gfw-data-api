from typing import Any, List

from asyncpg import UniqueViolationError
from fastapi import HTTPException

from ..models.orm.versions import Version as ORMVersion
from . import update_data


async def get_versions(dataset: str) -> List[ORMVersion]:
    versions: List[ORMVersion] = await ORMVersion.query.where(
        ORMVersion.dataset == dataset
    ).gino.all()

    return versions


async def get_version_names(dataset: str) -> List[Any]:
    versions: List[Any] = await ORMVersion.select("version").where(
        ORMVersion.dataset == dataset
    ).gino.all()

    return versions


async def get_version(dataset: str, version: str) -> ORMVersion:
    row: ORMVersion = await ORMVersion.get([dataset, version])
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Version with name {dataset}.{version} does not exist",
        )
    return row


async def create_version(dataset: str, version: str, **data) -> ORMVersion:
    try:
        new_version: ORMVersion = await ORMVersion.create(
            dataset=dataset, version=version, **data
        )
    except UniqueViolationError:
        raise HTTPException(
            status_code=400,
            detail=f"Version with name {dataset}.{version} already exists",
        )

    return new_version


async def update_version(dataset: str, version: str, **data):
    row: ORMVersion = await get_version(dataset, version)
    return await update_data(row, data)


async def delete_version(dataset: str, version: str) -> ORMVersion:
    row: ORMVersion = await get_version(dataset, version)
    await ORMVersion.delete.where(ORMVersion.dataset == dataset).where(
        ORMVersion.version == version
    ).gino.status()

    return row
