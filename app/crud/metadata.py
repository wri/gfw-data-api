from copy import deepcopy
from typing import List, Union

from asyncpg import UniqueViolationError
from h11 import Data

from ..models.orm.datasets import Dataset as ORMDataset
from ..models.orm.versions import Version as ORMVersion
from ..errors import RecordAlreadyExistsError, RecordNotFoundError
from . import datasets, versions
from ..models.orm.base import Base
from ..models.orm.dataset_metadata import DatasetMetadata
from ..models.orm.version_metadata import VersionMetadata
from ..models.enum import entity


async def create_dataset_metadata(dataset: str, **data) -> DatasetMetadata:
    """Create dataset metadata record"""
    d: ORMDataset = await datasets.get_dataset(dataset)
    if d is None:
        raise RecordNotFoundError(
            f"Cannot create metadata. Dataset with name {dataset} does not exist."
        )

    try:
        new_metadata: DatasetMetadata = await DatasetMetadata.create(
            dataset=dataset, **data)
    except UniqueViolationError:
        raise RecordAlreadyExistsError(
            f"Failed to create metadata. Dataset {dataset} has an existing metadata record."
        )

    return new_metadata


async def get_entity_metadata(metadata_id: str, entity_type: entity.EntityType) -> Union[DatasetMetadata, VersionMetadata]:
    """Get entity metadata by id"""

    metadata = None
    if entity_type == "dataset":
        metadata: DatasetMetadata = await DatasetMetadata.get([metadata_id])
    else:    # FIXME: add asset metadata logic
        metadata: VersionMetadata = await VersionMetadata.get([metadata_id])

    if metadata is None:
        raise RecordNotFoundError(f"Could not find requested metadata {metadata_id}")

    return metadata


async def create_version_metadata(dataset: str, version: str, **data):
    """Create version metadata record"""
    v: ORMVersion = await versions.get_version(dataset, version)
    if v is None:
        raise RecordNotFoundError(
            f"""Cannot create metadata. Either the dataset {dataset} or version {version}
            do not exist."""
        )

    try:
        new_metadata: VersionMetadata = await VersionMetadata.create(
            dataset=dataset, version=version, **data)
    except UniqueViolationError:
        raise RecordAlreadyExistsError(
            f"Failed to create metadata. Dataset {dataset} has an existing metadata record."
        )

    return new_metadata


def update_metadata(row: Base, parent: Base):
    """Dynamically update metadata with parent metadata.

    Make sure empty metadata get correctly merged.
    """

    if parent.metadata:
        _metadata = deepcopy(parent.metadata)
    else:
        _metadata = {}

    if row.metadata:
        filtered_metadata = {
            key: value for key, value in row.metadata.items() if value is not None
        }
    else:
        filtered_metadata = {}

    _metadata.update(filtered_metadata)
    row.metadata = _metadata
    return row


def update_all_metadata(rows: List[Base], parent: Base) -> List[Base]:
    """Updates metadata for a list of records."""
    new_rows = list()
    for row in rows:
        update_metadata(row, parent)
        new_rows.append(row)

    return new_rows
