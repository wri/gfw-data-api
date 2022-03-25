from copy import deepcopy
from typing import List, Union
from uuid import UUID

from asyncpg import UniqueViolationError

from app.models.enum.assets import AssetType

from ..errors import RecordAlreadyExistsError, RecordNotFoundError
from ..models.enum import entity
from ..models.orm.base import Base
from ..models.orm.dataset_metadata import DatasetMetadata as ORMDatasetMetadata
from ..models.orm.version_metadata import VersionMetadata as ORMVersionMetadata
from ..models.orm.asset_metadata import (
    AssetMetadata as ORMAssetMetadata,
    RasterBandMetadata as ORMRasterBandMetadata,
    FieldMetadata as ORMFieldMetadata
)
from ..models.orm.versions import Version as ORMVersion
# from ..models.orm.assets import Asset as ORMAsset
# from .assets import get_asset
from . import versions


async def create_dataset_metadata(dataset: str, **data) -> ORMDatasetMetadata:
    """Create dataset metadata record."""

    try:
        new_metadata: ORMDatasetMetadata = await ORMDatasetMetadata.create(
            dataset=dataset, **data
        )
    except UniqueViolationError:
        raise RecordAlreadyExistsError(
            f"Failed to create metadata. Dataset {dataset} has an existing metadata record."
        )

    return new_metadata


async def get_entity_metadata(
    metadata_id: str, entity_type: entity.EntityType
) -> Union[ORMDatasetMetadata, ORMVersionMetadata]:
    """Get entity metadata by id."""

    metadata = None
    if entity_type == "dataset":
        metadata: ORMDatasetMetadata = await ORMDatasetMetadata.get([metadata_id])
    elif entity_type == "version":
        metadata: ORMVersionMetadata = await ORMVersionMetadata.get([metadata_id])
    elif entity_type == "asset":
        metadata: ORMAssetMetadata = await ORMAssetMetadata.get([metadata_id])
    else:
        raise NotImplementedError(f"Entity type {entity_type} is not recognized.")

    if metadata is None:
        raise RecordNotFoundError(f"Could not find requested metadata {metadata_id}")

    return metadata


async def get_dataset_metadata(dataset: str) -> ORMDatasetMetadata:
    """Get dataset metadata."""
    metadata: ORMDatasetMetadata = await ORMDatasetMetadata.query.where(
        ORMDatasetMetadata.dataset == dataset
    ).gino.first()

    # if metadata is None:
    #     raise RecordNotFoundError(
    #         f"Could not find requested metadata dataset {dataset}"
    #     )

    return metadata


async def update_dataset_metadata(dataset: str, **data) -> ORMDatasetMetadata:
    metadata: ORMDatasetMetadata = await ORMDatasetMetadata.query.where(
        ORMDatasetMetadata.dataset == dataset
    ).gino.first()

    if metadata is None:
        raise RecordNotFoundError(
            f"Could not find requested metadata dataset {dataset}"
        )

    await metadata.update(**data).apply()

    return metadata


async def get_version_metadata(dataset: str, version: str) -> ORMVersionMetadata:
    """Get dataset version metadata."""
    metadata: ORMVersionMetadata = (
        await ORMVersionMetadata.load(dataset_metadata=ORMDatasetMetadata)
        .where(ORMVersionMetadata.dataset == dataset)
        .where(ORMVersionMetadata.version == version)
        .gino.first()
    )

    # if metadata is None:
    #     raise RecordNotFoundError(
    #         f"Could not find requested metadata dataset version {dataset}:{version}"
    #     )

    return metadata


async def create_version_metadata(dataset: str, version: str, **data):
    """Create version metadata record."""
    v: ORMVersion = await versions.get_version(dataset, version)
    if v is None:
        raise RecordNotFoundError(
            f"""Failed to create metadata. Either the dataset {dataset} or version {version}
            do not exist."""
        )

    dataset_metadata: ORMDatasetMetadata = await get_dataset_metadata(dataset)
    try:
        new_metadata: ORMVersionMetadata = await ORMVersionMetadata.create(
            dataset=dataset,
            version=version,
            dataset_metadata_id=dataset_metadata.id,
            **data,
        )
    except UniqueViolationError:
        raise RecordAlreadyExistsError(
            f"Failed to create metadata. Dataset {dataset} has an existing metadata record."
        )

    return new_metadata


async def delete_version_metadata(dataset: str, version: str) -> ORMVersionMetadata:
    """Delete version metadata."""
    metadata: ORMVersionMetadata = await get_version_metadata(dataset, version)
    await ORMVersionMetadata.delete.where(
        ORMVersionMetadata.dataset == dataset
    ).gino.status()

    return metadata


async def update_version_metadata(
    dataset: str, version: str, **data
) -> ORMVersionMetadata:
    """Update version metadata."""
    metadata: ORMVersionMetadata = await get_version_metadata(dataset, version)

    await metadata.update(**data).apply()

    return metadata


async def create_asset_metadata(asset_id: UUID, **data) -> ORMAssetMetadata:
    bands = data.pop("bands", None)
    fields = data.pop("fields", None)

    asset_metadata: ORMAssetMetadata = await ORMAssetMetadata.create(
        asset_id=asset_id, **data
    )

    bands_metadata = []
    if bands:
        for band in bands:
            band_metadata = await create_raster_band_metadata(asset_metadata.id, **band)
            bands_metadata.append(band_metadata)

        asset_metadata.bands = bands_metadata

    fields_metadata = []
    if fields:
        for field in fields:
            field_metadata = await create_field_metadata(asset_metadata.id, **field)
            fields_metadata.append(field_metadata)

        asset_metadata.fields = fields_metadata

    return asset_metadata


async def get_asset_metadata(asset_id: UUID):
    asset_metadata: ORMAssetMetadata = await ORMAssetMetadata.query.where(
        ORMAssetMetadata.asset_id == asset_id
    ).gino.first()

    bands: List[ORMRasterBandMetadata] = await ORMRasterBandMetadata.query.where(
        ORMRasterBandMetadata.asset_metadata_id == asset_metadata.id
    ).gino.all()
    print("bands", len(bands))
    if bands:
        asset_metadata.bands = bands

    fields: List[ORMFieldMetadata] = await ORMFieldMetadata.query.where(
        ORMFieldMetadata.asset_metadata_id == asset_metadata.id
    ).gino.all()
    if fields:
        asset_metadata.fields = fields

    return asset_metadata


async def create_raster_band_metadata(asset_metadata_id: UUID, **data):
    raster_band_metadata: ORMRasterBandMetadata = await ORMRasterBandMetadata.create(
        asset_metadata_id=asset_metadata_id, **data
    )

    return raster_band_metadata


async def create_field_metadata(asset_metadata_id: UUID, **data):
    field_metadata: ORMFieldMetadata = await ORMFieldMetadata.create(
        asset_metadata_id=asset_metadata_id,
        **data
    )

    return field_metadata


async def get_asset_fields(asset_id: UUID):
    field_metadata: List[ORMFieldMetadata] = await (
        ORMFieldMetadata.join(ORMAssetMetadata)
        .select()
        .with_only_columns(
            [
                ORMFieldMetadata.id,
                ORMFieldMetadata.asset_metadata_id,
                ORMFieldMetadata.alias,
                ORMFieldMetadata.created_on,
                ORMFieldMetadata.data_type,
                ORMFieldMetadata.description,
                ORMFieldMetadata.is_feature_info,
                ORMFieldMetadata.is_filter,
                ORMFieldMetadata.updated_on,
                ORMFieldMetadata.name,
                ORMFieldMetadata.unit
            ]
        )
        .where(ORMAssetMetadata.asset_id == asset_id)
    ).gino.all()

    return field_metadata


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
