import json
from datetime import datetime
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi.encoders import jsonable_encoder

from tests.routes import create_asset

version_metadata = {
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

version_data = {
    "is_latest": True,
    "source_type": "vector",
    "source_uri": ["s3://some/path"],
    "metadata": version_metadata,
    "creation_options": {"src_driver": "ESRI Shapefile", "zipped": True},
}


def test_tasks(client, db):
    """Basic test to make sure task routes behave correctly."""
    # Add a dataset, version, and asset
    dataset = "test"
    version = "v20200626"
    asset_type = "Database table"
    asset_uri = "s3://path/to/file"

    # with patch("fastapi.BackgroundTasks.add_task", return_value=None):
    asset = create_asset(client, dataset, version, asset_type, asset_uri)
    asset_id = asset["asset_id"]

    # # Now create a single task
    # new_task_id = uuid4()
    # task_payload = {
    #     "asset_id": asset_id,
    #     "change_log": [
    #         {
    #             "date_time": str(datetime.now()),
    #             "status": "pending",
    #             "message": f"Scheduled job {new_task_id}",
    #             "detail": f"Job ID: {new_task_id}",
    #         }
    #     ],
    # }
    # create_resp = client.put(f"/tasks/{new_task_id}", data=json.dumps(task_payload))
    # assert create_resp.json()["status"] == "success"
    # # Assert on response structure and content

    existing_tasks = client.get(f"/tasks/assets/{asset_id}").json()["data"]
    for task in existing_tasks:
        assert len(task["change_log"]) == 1
        print(task["change_log"][0]["status"])

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
    assert 1 == 2

    # Make sure that changelogs were concatenated, now 2 of them
    # Make sure that asset, version status still "pending"

    # Send an HTTP PATCH with a "failed" changelog
    # patch_resp = client.patch(f"/meta/tasks/{new_task_id}", )
    # Make sure that asset, version status become "failed"
