from typing import Any, Awaitable, Callable, Coroutine, List
from urllib.parse import urljoin
from uuid import UUID

from ..application import ContextEngine
from ..crud import assets as crud_assets
from ..crud import tasks as crud_tasks
from ..models.orm.assets import Asset as ORMAsset
from ..models.orm.tasks import Task as ORMTask
from ..models.pydantic.change_log import ChangeLog
from ..settings.globals import (
    API_URL,
    AWS_REGION,
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
    {"name": "STATUS_URL", "value": urljoin(API_URL, "task")},
    {"name": "SERVICE_ACCOUNT_TOKEN", "value": SERVICE_ACCOUNT_TOKEN},
    {"name": "AWS_REGION", "value": AWS_REGION},
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


Callback = Callable[[UUID, ChangeLog], Coroutine[Any, Any, Awaitable[None]]]


def callback_constructor(
    asset_id: UUID,
) -> Callback:
    """Callback constructor.

    Assign asset_id in the context of the constructor once. Afterwards
    you will only need to pass the ChangeLog object.
    """

    async def callback(task_id: UUID, change_log: ChangeLog) -> ORMAsset:
        async with ContextEngine("WRITE"):
            task: ORMTask = await crud_tasks.create_task(
                task_id, asset_id=asset_id, change_log=[change_log.dict(by_alias=True)]
            )

        return task

    return callback
