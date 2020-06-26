from typing import Any, Dict, List
from urllib.parse import urljoin

from ..application import ContextEngine, db
from ..crud import assets as crud_assets
from ..models.orm.queries.fields import fields
from ..models.pydantic.metadata import FieldMetadata
from ..settings.globals import (
    API_URL,
    READER_DBNAME,
    READER_HOST,
    READER_PASSWORD,
    READER_PORT,
    READER_USERNAME,
    SERVICE_ACCOUNT_TOKEN,
    WRITER_DBNAME,
    WRITER_HOST,
    WRITER_PASSWORD,
    WRITER_PORT,
    WRITER_USERNAME,
)

report_vars: List = [
    {"name": "STATUS_URL", "value": urljoin(API_URL, "tasks")},
    {"name": "SERVICE_ACCOUNT_TOKEN", "value": SERVICE_ACCOUNT_TOKEN},
]

writer_secrets: List = [
    {"name": "PGPASSWORD", "value": str(WRITER_PASSWORD)},
    {"name": "PGHOST", "value": WRITER_HOST},
    {"name": "PGPORT", "value": WRITER_PORT},
    {"name": "PGDATABASE", "value": WRITER_DBNAME},
    {"name": "PGUSER", "value": WRITER_USERNAME},
] + report_vars

reader_secrets: List = [
    {"name": "PGPASSWORD", "value": str(READER_PASSWORD)},
    {"name": "PGHOST", "value": READER_HOST},
    {"name": "PGPORT", "value": READER_PORT},
    {"name": "PGDATABASE", "value": READER_DBNAME},
    {"name": "PGUSER", "value": READER_USERNAME},
] + report_vars


async def get_field_metadata(dataset: str, version: str) -> List[Dict[str, Any]]:
    """Get field list for asset and convert into Metadata object."""

    rows = await db.all(fields, dataset=dataset, version=version)
    field_metadata = list()

    for row in rows:
        metadata = FieldMetadata.from_orm(row)
        if metadata.field_name_ in ["geom", "geom_wm", "gfw_geojson", "gfw_bbox"]:
            metadata.is_filter = False
            metadata.is_feature_info = False
        metadata.field_alias = metadata.field_name_
        field_metadata.append(metadata.dict())

    return field_metadata


async def update_asset_status(asset_id, status):
    """Update status of asset."""

    async with ContextEngine("WRITE"):
        await crud_assets.update_asset(asset_id, status=status)


async def update_asset_field_metadata(dataset, version, asset_id):
    """Update asset field metadata."""

    field_metadata: List[Dict[str, Any]] = await get_field_metadata(dataset, version)
    metadata = {"fields_": field_metadata}

    async with ContextEngine("WRITE"):
        await crud_assets.update_asset(asset_id, metadata=metadata)
