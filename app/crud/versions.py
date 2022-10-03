from typing import Any, Dict, List, Optional

from asyncpg import UniqueViolationError

from ..errors import RecordAlreadyExistsError, RecordNotFoundError
from ..models.orm.assets import Asset as ORMAsset
from ..models.orm.datasets import Dataset as ORMDataset
from ..models.orm.version_metadata import VersionMetadata as ORMVersionMetadata
from ..models.orm.versions import Version as ORMVersion
from ..utils.generators import list_to_async_generator
from . import datasets, update_data
from .metadata import (
    create_version_metadata,
    update_all_metadata,
    update_version_metadata,
)


async def get_versions(dataset: str) -> List[ORMVersion]:
    versions: List[ORMVersion] = await ORMVersion.query.where(
        ORMVersion.dataset == dataset
    ).gino.all()

    return versions


async def get_version_names(dataset: str) -> List[Any]:
    versions: List[Any] = (
        await ORMVersion.select("version")
        .where(ORMVersion.dataset == dataset)
        .gino.all()
    )

    return versions


async def get_version(dataset: str, version: str) -> ORMVersion:
    row: ORMVersion = (
        await ORMVersion.load(metadata=ORMVersionMetadata)
        .where(ORMVersion.dataset == dataset)
        .where(ORMVersion.version == version)
        .gino.first()
    )

    if row is None:
        raise RecordNotFoundError(
            f"Version with name {dataset}.{version} does not exist"
        )

    row.metadata = getattr(row, "metadata", {})

    return row


async def get_latest_version(dataset) -> str:
    """Fetch latest version number."""

    latest: Optional[str] = (
        await ORMVersion.select("version")
        .where(ORMVersion.dataset == dataset)
        .where(ORMVersion.is_latest)
        .gino.scalar()
    )

    if latest is None:
        raise RecordNotFoundError(f"Dataset {dataset} has no latest version.")

    return latest


async def create_version(dataset: str, version: str, **data) -> ORMVersion:
    """Create new version record if version does not yet exist."""
    d: ORMDataset = await datasets.get_dataset(dataset)
    if d is None:
        raise RecordNotFoundError(
            f"Cannot create version. Dataset with name {dataset} does not exist."
        )

    # default to dataset.is_downloadable if not set
    if data.get("is_downloadable") is None:
        data["is_downloadable"] = d.is_downloadable

    if data.get("is_latest"):
        await _reset_is_latest(dataset, version)

    metadata_data = data.pop("metadata", None)
    try:
        new_version: ORMVersion = await ORMVersion.create(
            dataset=dataset, version=version, **data
        )
        new_version.metadata = {}
    except UniqueViolationError:
        raise RecordAlreadyExistsError(
            f"Version with name {dataset}.{version} already exists."
        )

    if metadata_data:
        metadata: ORMVersionMetadata = await create_version_metadata(
            dataset, version, **metadata_data
        )
        new_version.metadata = metadata

    return new_version


async def update_version(dataset: str, version: str, **data) -> ORMVersion:
    """Update fields of version."""
    row: ORMVersion = await get_version(dataset, version)
    metadata_data = data.pop("metadata", None)
    row = await update_data(row, data)

    if metadata_data:
        try:
            metadata = await update_version_metadata(dataset, version, **metadata_data)
        except RecordNotFoundError:
            metadata = await create_version_metadata(dataset, version, **metadata_data)
        row.metadata = metadata

    await _update_is_downloadable(dataset, version, data)

    if data.get("is_latest"):
        await _reset_is_latest(dataset, version)

    return row


async def delete_version(dataset: str, version: str) -> ORMVersion:
    """Delete a version."""
    v: ORMVersion = await get_version(dataset, version)
    await ORMVersion.delete.where(ORMVersion.dataset == dataset).where(
        ORMVersion.version == version
    ).gino.status()

    return v


async def _update_is_downloadable(
    dataset: str, version: str, data: Dict[str, Any]
) -> None:
    """Populate is_downloadable attribute to all downstream assets.

    Using gino loader instead of own crud methods to avoid circular
    imports.
    """
    if data.get("is_downloadable") is not None:

        # I tried using gino.iterate() instead of creating a generator
        # however this somehow throw an error: No Connection in context, please provide one.
        assets = await ORMAsset.query.where(
            ORMAsset.dataset == dataset and ORMAsset.version == version
        ).gino.all()
        asset_gen = list_to_async_generator(assets)
        async for asset in asset_gen:
            await asset.update(is_downloadable=data.get("is_downloadable")).apply()


async def _reset_is_latest(dataset: str, version: str) -> None:
    versions = await get_versions(dataset)
    version_gen = list_to_async_generator(versions)
    async for version_orm in version_gen:
        if version_orm.version != version:
            await update_version(dataset, version_orm.version, is_latest=False)
