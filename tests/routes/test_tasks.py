from datetime import datetime
from uuid import uuid4

import pytest

from app.application import ContextEngine
from app.crud.assets import create_asset
from app.crud.datasets import create_dataset
from app.crud.tasks import create_task
from app.crud.versions import create_version

dataset_payload = {
    "metadata": {
        "title": "string",
        "subtitle": "string",
        "function": "string",
        "resolution": "string",
        "geographic_coverage": "string",
        "source": "string",
        "update_frequency": "string",
        "cautions": "string",
        "license": "string",
        "overview": "string",
        "citation": "string",
        "tags": ["string"],
        "data_language": "string",
        "key_restrictions": "string",
        "scale": "string",
        "added_date": "string",
        "why_added": "string",
        "other": "string",
        "learn_more": "string",
    }
}

version_payload = {
    "is_latest": True,
    "source_type": "vector",
    "source_uri": ["s3://some/path"],
    "metadata": dataset_payload["metadata"],
    "creation_options": {"src_driver": "ESRI Shapefile", "zipped": True},
}


# @patch("fastapi.BackgroundTasks.add_task", return_value=None)
@pytest.mark.asyncio
async def test_tasks(client, db):
    """Basic test to make sure task routes behave correctly."""

    # dataset = "test"
    # _ = client.put(f"/meta/{dataset}", data=json.dumps(dataset_payload))
    #
    # version = "v1.1.1"
    # _ = client.put(
    #     f"/meta/{dataset}/{version}", data=json.dumps(version_payload)
    # )

    # Add a dataset, version, and asset
    dataset = "test"
    version = "v1.1.1"
    async with ContextEngine("WRITE"):
        _ = await create_dataset(dataset)
        _ = await create_version(dataset, version, source_type="table")
        new_asset = await create_asset(
            dataset,
            version,
            asset_type="Database table",
            asset_uri="s3://path/to/file",
        )

    asset_id = new_asset.asset_id

    new_task_id = uuid4()
    async with ContextEngine("WRITE"):
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
    # changelog = {
    #     "date_time": "2020-06-25 14:30:00",
    #     "status": "pending",
    #     "detail": "None"
    # }
    # patch_resp = client.patch(f"/meta/tasks/{new_task_id}", data=json.dumps(changelog))
    # print(patch_resp.json())

    # Make sure that changelogs were concatenated, now 2 of them
    # Make sure that asset, version status still "pending"

    # Send an HTTP PATCH with a "failed" changelog
    # patch_resp = client.patch(f"/meta/tasks/{new_task_id}", )
    # Make sure that asset, version status become "failed"
