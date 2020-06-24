from datetime import datetime
from uuid import uuid4

import pytest

from app.application import ContextEngine
from app.crud.assets import create_asset
from app.crud.datasets import create_dataset
from app.crud.tasks import create_task
from app.crud.versions import create_version


@pytest.mark.asyncio
async def test_tasks(client, db):
    """
    Basic test to make sure tasks route behaves correctly
    """

    dataset_name = "test"
    version_name = "v1.1.1"

    # Add a dataset, version, and asset
    async with ContextEngine("PUT"):
        _ = await create_dataset(dataset_name)
        _ = await create_version(dataset_name, version_name, source_type="table")
        new_asset = await create_asset(
            dataset_name,
            version_name,
            asset_type="Database table",
            asset_uri="s3://path/to/file",
        )

    asset_id = new_asset.asset_id

    new_task_id = uuid4()
    async with ContextEngine("PUT"):
        new_task = await create_task(
            new_task_id,
            asset_id=asset_id,
            change_log=[
                {
                    "date_time": datetime.now(),
                    "status": "pending",
                    "message": f"Scheduled job {new_task_id}",
                    "detail": f"Job ID: {new_task_id}",
                }
            ],
        )
    assert new_task.asset_id == asset_id

    # Do an HTTP GET to check structure and content of response
    # get_resp = client.get(f"/meta/tasks/{new_task_id}")
    # Assert on the structure + content

    # Send an HTTP PATCH with another "pending" changelog
    # patch_resp = client.patch(f"/meta/tasks/{new_task_id}", )
    # Make sure that changelogs were concatenated, now 2 of them
    # Make sure that asset, version status still "pending"

    # Send an HTTP PATCH with a "failed" changelog
    # patch_resp = client.patch(f"/meta/tasks/{new_task_id}", )
    # Make sure that asset, version status become "failed"
