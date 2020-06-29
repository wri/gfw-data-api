from typing import Any, List, Optional

from asyncpg import UniqueViolationError
from fastapi import HTTPException

from ..models.orm.datasets import Dataset as ORMDataset
from ..models.orm.versions import Version as ORMVersion
from . import datasets, update_all_metadata, update_data, update_metadata


async def get_versions(dataset: str) -> List[ORMVersion]:
    versions: List[ORMVersion] = await ORMVersion.query.where(
        ORMVersion.dataset == dataset
    ).gino.all()
    d: ORMDataset = await datasets.get_dataset(dataset)

    return update_all_metadata(versions, d)


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
    d: ORMDataset = await datasets.get_dataset(dataset)

    return update_metadata(row, d)


async def get_latest_version(dataset) -> str:
    """Fetch latest version number."""

    latest: Optional[str] = await ORMVersion.select("version").where(
        ORMVersion.dataset == dataset
    ).where(ORMVersion.is_latest).gino.scalar()

    if latest is None:
        raise HTTPException(
            status_code=400, detail=f"Dataset {dataset} has no latest version."
        )

    return latest


async def create_version(dataset: str, version: str, **data) -> ORMVersion:
    """Create new version record if version does not yet exist."""
    try:
        new_version: ORMVersion = await ORMVersion.create(
            dataset=dataset, version=version, **data
        )
    except UniqueViolationError:
        raise HTTPException(
            status_code=400,
            detail=f"Version with name {dataset}.{version} already exists",
        )
    d: ORMDataset = await datasets.get_dataset(dataset)

    return update_metadata(new_version, d)


async def update_version(dataset: str, version: str, **data):
    """Update fields of version."""
    row: ORMVersion = await get_version(dataset, version)
    row = await update_data(row, data)

    d: ORMDataset = await datasets.get_dataset(dataset)

    return update_metadata(row, d)


async def delete_version(dataset: str, version: str) -> ORMVersion:
    """Delete a version."""
    row: ORMVersion = await get_version(dataset, version)
    await ORMVersion.delete.where(ORMVersion.dataset == dataset).where(
        ORMVersion.version == version
    ).gino.status()

    d: ORMDataset = await datasets.get_dataset(dataset)

    return update_metadata(row, d)
