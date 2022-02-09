from typing import Any, Dict, List

from asyncpg import UniqueViolationError

from ..application import db
from ..errors import RecordAlreadyExistsError, RecordNotFoundError
from ..models.orm.assets import Asset as ORMAsset
from ..models.orm.dataset_metadata import DatasetMetadata as ORMDatasetMetadata
from ..models.orm.datasets import Dataset as ORMDataset
from ..models.orm.queries.datasets import all_datasets
from ..models.orm.versions import Version as ORMVersion
from ..utils.generators import list_to_async_generator
from . import metadata as metadata_crud
from . import update_data


async def get_datasets() -> List[ORMDataset]:
    """Get list of all datasets."""

    rows = await db.all(all_datasets)
    return rows


async def get_dataset(dataset: str) -> ORMDataset:
    row: ORMDataset = await ORMDataset.get(dataset)
    metadata = await metadata_crud.get_dataset_metadata(dataset)
    row.metadata = metadata

    if row is None:
        raise RecordNotFoundError(f"Dataset with name {dataset} does not exist")

    return row


async def create_dataset(dataset: str, **data) -> ORMDataset:
    try:
        new_dataset: ORMDataset = await ORMDataset.create(dataset=dataset, **data)
        metadata_data = data.pop("metadata")
        if metadata_data:
            metadata: ORMDatasetMetadata = await metadata_crud.create_dataset_metadata(
                dataset, **metadata_data
            )
            new_dataset.metadata = metadata
    except UniqueViolationError:
        raise RecordAlreadyExistsError(f"Dataset with name {dataset} already exists")

    return new_dataset


async def update_dataset(dataset: str, **data) -> ORMDataset:
    row: ORMDataset = await get_dataset(dataset)
    new_row = await update_data(row, data)

    await _update_is_downloadable(dataset, data)

    return new_row


async def delete_dataset(dataset: str) -> ORMDataset:
    row: ORMDataset = await get_dataset(dataset)
    await ORMDataset.delete.where(ORMDataset.dataset == dataset).gino.status()

    return row


async def _update_is_downloadable(dataset: str, data: Dict[str, Any]) -> None:
    """Populate is_downloadable attribute to all downstream versions and
    assets.

    Using gino loader instead of own crud methods to avoid circular
    imports.
    """
    if data.get("is_downloadable") is not None:

        # I tried using gino.iterate() instead of creating a generator
        # however this somehow throw an error: No Connection in context, please provide one.
        versions = await ORMVersion.query.where(
            ORMVersion.dataset == dataset
        ).gino.all()
        version_gen = list_to_async_generator(versions)
        async for version in version_gen:
            await version.update(is_downloadable=data.get("is_downloadable")).apply()

            assets = await ORMAsset.query.where(
                ORMAsset.dataset == dataset and ORMAsset.version == version
            ).gino.all()
            asset_gen = list_to_async_generator(assets)
            async for asset in asset_gen:
                await asset.update(is_downloadable=data.get("is_downloadable")).apply()
