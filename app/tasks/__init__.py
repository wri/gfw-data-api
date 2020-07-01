from typing import List
from urllib.parse import urljoin

from ..application import ContextEngine
from ..crud import assets as crud_assets
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


async def update_asset_status(asset_id, status):
    """Update status of asset."""

    async with ContextEngine("WRITE"):
        await crud_assets.update_asset(asset_id, status=status)
