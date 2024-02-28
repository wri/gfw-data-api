from copy import deepcopy
from typing import List
from uuid import UUID

from asyncpg import UniqueViolationError

from ..errors import RecordAlreadyExistsError, RecordNotFoundError
from ..models.orm.asset_metadata import AssetMetadata as ORMAssetMetadata
from ..models.orm.asset_metadata import FieldMetadata as ORMFieldMetadata
from ..models.orm.asset_metadata import RasterBandMetadata as ORMRasterBandMetadata
from ..models.orm.assets import Asset as ORMAsset
from ..models.orm.base import Base
from ..models.orm.dataset_metadata import DatasetMetadata as ORMDatasetMetadata
from ..models.orm.version_metadata import VersionMetadata as ORMVersionMetadata
from ..models.pydantic.asset_metadata import FieldMetadataOut


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


async def get_dataset_metadata(dataset: str) -> ORMDatasetMetadata:
    """Get dataset metadata."""
    metadata: ORMDatasetMetadata = await ORMDatasetMetadata.query.where(
        ORMDatasetMetadata.dataset == dataset
    ).gino.first()

    if metadata is None:
        raise RecordNotFoundError(
            f"Could not find requested metadata dataset {dataset}"
        )

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
        await ORMVersionMetadata.query.where(ORMVersionMetadata.dataset == dataset)
        .where(ORMVersionMetadata.version == version)
        .gino.first()
    )

    if metadata is None:
        raise RecordNotFoundError(
            f"Could not find requested metadata for dataset version {dataset}:{version}"
        )

    return metadata


async def create_version_metadata(dataset: str, version: str, **data):
    """Create version metadata record."""

    content_date_range = data.pop("content_date_range", None)
    if content_date_range:
        data["content_start_date"] = content_date_range.get("start_date")
        data["content_end_date"] = content_date_range.get("end_date")

    try:
        new_metadata: ORMVersionMetadata = await ORMVersionMetadata.create(
            dataset=dataset,
            version=version,
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
    content_date_range = data.pop("content_date_range", None)
    if content_date_range:
        data["content_start_date"] = content_date_range.get(
            "start_date", metadata.content_start_date
        )
        data["content_end_date"] = content_date_range.get(
            "end_date", metadata.content_end_date
        )

    await metadata.update(**data).apply()

    return metadata


async def create_asset_metadata(asset_id: UUID, **data) -> ORMAssetMetadata:
    bands = data.pop("bands", None)
    fields = data.pop("fields", None)

    try:
        asset_metadata: ORMAssetMetadata = await ORMAssetMetadata.create(
            asset_id=asset_id, **data
        )
    except UniqueViolationError:
        raise RecordAlreadyExistsError(
            f"Failed to create metadata. Asset {asset_id} has an existing metadata record."
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

    if asset_metadata is None:
        raise RecordNotFoundError(f"No metadata found for asset {asset_id}.")

    bands: List[ORMRasterBandMetadata] = await ORMRasterBandMetadata.query.where(
        ORMRasterBandMetadata.asset_metadata_id == asset_metadata.id
    ).gino.all()

    if bands:
        asset_metadata.bands = bands

    asset_metadata.fields = await get_asset_fields(asset_metadata.id)

    return asset_metadata


async def update_asset_metadata(asset_id: UUID, **data) -> ORMAssetMetadata:
    """Update asset metadata."""
    fields = data.pop("fields", None)

    asset_metadata: ORMAssetMetadata = await get_asset_metadata(asset_id)

    if data:
        await asset_metadata.update(**data).apply()

    fields_metadata = []
    if fields:
        for field in fields:
            try:
                field_metadata = await update_field_metadata(
                    asset_metadata.id, field["name"], **field
                )
            except RecordNotFoundError:
                field_metadata = await create_field_metadata(asset_metadata.id, **field)
            fields_metadata.append(field_metadata)

        asset_metadata.fields = fields_metadata

    return asset_metadata


async def delete_asset_metadata(asset_id: UUID) -> ORMAssetMetadata:
    asset_metadata: ORMAssetMetadata = await get_asset_metadata(asset_id)
    await ORMAssetMetadata.delete.where(
        ORMAssetMetadata.asset_id == asset_id
    ).gino.status()

    return asset_metadata


async def create_raster_band_metadata(
    asset_metadata_id: UUID, **data
) -> ORMRasterBandMetadata:
    raster_band_metadata: ORMRasterBandMetadata = await ORMRasterBandMetadata.create(
        asset_metadata_id=asset_metadata_id, **data
    )

    return raster_band_metadata


async def create_field_metadata(asset_metadata_id: UUID, **data) -> ORMFieldMetadata:
    field_metadata: ORMFieldMetadata = await ORMFieldMetadata.create(
        asset_metadata_id=asset_metadata_id, **data
    )

    return field_metadata


async def update_field_metadata(
    metadata_id: UUID, field_name: str, **data
) -> ORMFieldMetadata:
    field_metadata: ORMFieldMetadata = await get_asset_field(metadata_id, field_name)

    await field_metadata.update(**data).apply()

    return field_metadata


async def get_asset_fields(asset_metadata_id: UUID) -> List[ORMFieldMetadata]:
    fields_metadata: List[ORMFieldMetadata] = await (
        ORMFieldMetadata.query.where(
            ORMFieldMetadata.asset_metadata_id == asset_metadata_id
        )
    ).gino.all()

    return fields_metadata


async def get_asset_fields_dicts(asset: ORMAsset):
    if not asset.metadata:
        return []

    fields = [
        FieldMetadataOut.from_orm(field).dict() for field in asset.metadata.fields
    ]

    return fields


async def get_asset_field(asset_metadata_id: UUID, field_name: str) -> ORMFieldMetadata:
    field_metadata: ORMFieldMetadata = await ORMFieldMetadata.get(
        [asset_metadata_id, field_name]
    )

    if field_metadata is None:
        raise RecordNotFoundError("No field metadata record found.")

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
