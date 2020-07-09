import uuid
from unittest.mock import patch

import pytest

from tests.routes import create_default_asset, generate_uuid


@pytest.mark.asyncio
async def test_assets(async_client):
    """Basic tests of asset endpoint behavior."""
    # Add a dataset, version, and default asset
    dataset = "test"
    version = "v20200626"

    with patch("app.tasks.batch.submit_batch_job", side_effect=generate_uuid):
        asset = await create_default_asset(async_client, dataset, version)
    asset_id = asset["asset_id"]

    # Verify that the asset and version are in state "pending"
    version_resp = await async_client.get(f"/dataset/{dataset}/{version}")
    assert version_resp.json()["data"]["status"] == "pending"

    asset_resp = await async_client.get(f"/asset/{asset_id}")
    assert asset_resp.json()["data"]["status"] == "pending"

    # Try adding a non-default asset, which shouldn't work while the version
    # is still in "pending" status
    asset_payload = {
        "asset_type": "Database table",
        "asset_uri": "http://www.slashdot.org",
        "is_managed": False,
        "creation_options": {
            "zipped": False,
            "src_driver": "GeoJSON",
            "delimiter": ",",
        },
    }
    create_asset_resp = await async_client.post(
        f"/dataset/{dataset}/{version}/assets", json=asset_payload
    )
    assert create_asset_resp.json()["status"] == "failed"
    assert create_asset_resp.json()["message"] == (
        "Version status is currently `pending`. "
        "Please retry once version is in status `saved`"
    )

    # Now add a task changelog of status "failed" which should make the
    # version status "failed". Try to add a non-default asset again, which
    # should fail as well but with a different explanation.
    get_resp = await async_client.get(f"/asset/{asset_id}/tasks")
    tasks = get_resp.json()["data"]
    sample_task_id = tasks[0]["task_id"]
    patch_payload = {
        "change_log": [
            {
                "date_time": "2020-06-25 14:30:00",
                "status": "failed",
                "message": "Bad Luck!",
                "detail": "None",
            }
        ]
    }
    patch_resp = await async_client.patch(
        f"/tasks/{sample_task_id}", json=patch_payload
    )
    assert patch_resp.json()["status"] == "success"

    create_asset_resp = await async_client.post(
        f"/dataset/{dataset}/{version}/assets", json=asset_payload
    )
    assert create_asset_resp.json()["status"] == "failed"
    assert create_asset_resp.json()["message"] == (
        "Version status is `failed`. Cannot add any assets."
    )
