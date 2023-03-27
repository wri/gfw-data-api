import json
from unittest.mock import patch

import pytest

from app.models.enum.assets import AssetType
from tests.utils import create_default_asset, generate_uuid


@pytest.mark.asyncio
async def test_tasks_success(async_client):
    """Verify that all tasks succeeding -> 'saved' status for default
    asset/version.

    After that, make sure adding auxiliary assets doesn't affect version
    status.
    """
    # Add a dataset, version, and default asset
    dataset = "test"
    version = "v20200626"

    asset = await create_default_asset(
        dataset, version, async_client=async_client, execute_batch_jobs=False
    )
    asset_id = asset["asset_id"]

    # Verify that the asset and version are in state "pending"
    version_resp = await async_client.get(f"/dataset/{dataset}/{version}")
    assert version_resp.json()["data"]["status"] == "pending"

    asset_resp = await async_client.get(f"/asset/{asset_id}")
    assert asset_resp.json()["data"]["status"] == "pending"

    # At this point there should be a bunch of tasks rows for the default
    # asset, though we've mocked out the actual creation of Batch jobs.
    # That's fine, we're going to update the task rows via the task status
    # endpoint the same way the Batch tasks would (though via the test
    # client instead of curl).

    # Verify the existence of the tasks, and that they each have only the
    # initial changelog with status "pending"
    get_resp = await async_client.get(f"/asset/{asset_id}/tasks")
    existing_tasks = get_resp.json()["data"]

    assert len(existing_tasks) == 8
    for task in existing_tasks:
        assert len(task["change_log"]) == 1
        assert task["change_log"][0]["status"] == "pending"

    # Arbitrarily choose a task and add a changelog.
    sample_task_id = existing_tasks[0]["task_id"]
    patch_payload = {
        "change_log": [
            {
                "date_time": "2020-06-25 14:30:00",
                "status": "success",
                "message": "All finished!",
                "detail": "None",
            }
        ]
    }
    patch_resp = await async_client.patch(f"/task/{sample_task_id}", json=patch_payload)
    assert patch_resp.json()["status"] == "success"

    # Verify the task has two changelogs now.
    get_resp = await async_client.get(f"/task/{sample_task_id}")
    assert len(get_resp.json()["data"]["change_log"]) == 2

    # Verify that the asset and version are still in state "pending"
    version_resp = await async_client.get(f"/dataset/{dataset}/{version}")
    assert version_resp.json()["data"]["status"] == "pending"

    asset_resp = await async_client.get(f"/asset/{asset_id}")
    assert asset_resp.json()["data"]["status"] == "pending"

    # Update the rest of the tasks with changelogs of status "success"
    # Verify that the completion status is propagated to the asset and version
    for task in existing_tasks[1:]:
        patch_payload = {
            "change_log": [
                {
                    "date_time": "2020-06-25 14:30:00",
                    "status": "success",
                    "message": "All finished!",
                    "detail": "None",
                }
            ]
        }
        patch_resp = await async_client.patch(
            f"/task/{task['task_id']}", json=patch_payload
        )
        assert patch_resp.json()["status"] == "success"

    version_resp = await async_client.get(f"/dataset/{dataset}/{version}")
    assert version_resp.json()["data"]["status"] == "saved"

    asset_resp = await async_client.get(f"/asset/{asset_id}")
    assert asset_resp.json()["data"]["status"] == "saved"

    # Verify if the dynamic vector tile cache was created. Status should be failed b/c batch jobs were not triggered.
    assets_resp = await async_client.get(f"/dataset/{dataset}/{version}/assets")
    assert len(version_resp.json()["data"]["assets"]) == 1
    assert len(assets_resp.json()["data"]) == 2
    assert assets_resp.json()["data"][0]["asset_type"] == AssetType.geo_database_table
    assert (
        assets_resp.json()["data"][1]["asset_type"]
        == AssetType.dynamic_vector_tile_cache
    )
    assert assets_resp.json()["data"][1]["status"] == "failed"

    # The following will fail until creation of auxiliary assets is working

    # Commenting-out the following as "fields" is commented-out in the metadata model
    # Looks like it must have been silently ignored until now, but I'm making all
    # models strict (setting extra=Forbid) and this now causes a failure. Can
    # reinstate if we ever add "fields" to the metadata model
    #
    # field_payload = {
    #     "metadata": {
    #         "fields": [
    #             {"field_name": "test", "field_type": "numeric", "is_feature_info": True}
    #         ]
    #     }
    # }
    #
    # asset_resp = await async_client.patch(f"/asset/{asset_id}", json=field_payload)
    # print(asset_resp.json())
    # assert asset_resp.json()["status"] == "success"

    # Now that the default asset is saved we can create a non-default
    # asset, which is handled slightly differently. In particular if
    # creation of the auxiliary asset fails the version remains in the
    # "saved" state. Let's verify that.
    asset_payload = {
        "asset_type": "Static vector tile cache",
        "asset_uri": "http://www.humptydumpty.org",
        "is_managed": False,
        "creation_options": {
            "min_zoom": 0,
            "max_zoom": 9,
            "tile_strategy": "discontinuous",
            "layer_style": [],
        },
    }
    with patch("app.tasks.batch.submit_batch_job", side_effect=generate_uuid):
        create_asset_resp = await async_client.post(
            f"/dataset/{dataset}/{version}/assets", json=asset_payload
        )
    print(json.dumps(create_asset_resp.json(), indent=2))
    assert create_asset_resp.json()["status"] == "success"
    asset_id = create_asset_resp.json()["data"]["asset_id"]

    # Verify there are three assets now,
    # including the implicitly created ndjson asset
    get_resp = await async_client.get(f"/dataset/{dataset}/{version}/assets")
    assert len(get_resp.json()["data"]) == 4

    # Verify the existence of tasks for the new asset
    get_resp = await async_client.get(f"/asset/{asset_id}/tasks")
    non_default_tasks = get_resp.json()["data"]
    assert len(non_default_tasks) == 1
    for task in non_default_tasks:
        assert len(task["change_log"]) == 1
        assert task["change_log"][0]["status"] == "pending"

    # Arbitrarily choose a task and add a changelog with status "failed"
    sample_task_id = non_default_tasks[0]["task_id"]
    patch_payload = {
        "change_log": [
            {
                "date_time": "2020-06-25 14:30:00",
                "status": "failed",
                "message": "Womp womp!",
                "detail": "None",
            }
        ]
    }
    patch_resp = await async_client.patch(f"/task/{sample_task_id}", json=patch_payload)
    assert patch_resp.json()["status"] == "success"

    # Verify the asset status is now "failed"
    get_resp = await async_client.get(f"/asset/{asset_id}")
    assert get_resp.json()["data"]["status"] == "failed"

    # ... but that the version status is still "saved"
    get_resp = await async_client.get(f"/dataset/{dataset}/{version}")
    assert get_resp.json()["data"]["status"] == "saved"


@pytest.mark.asyncio
async def test_tasks_failure(async_client):
    """Verify that failing tasks result in failed asset/version."""
    # Add a dataset, version, and default asset
    dataset = "test"
    version = "v20200626"

    asset = await create_default_asset(
        dataset, version, async_client=async_client, execute_batch_jobs=False
    )
    asset_id = asset["asset_id"]

    # Verify that the asset and version are in state "pending"
    version_resp = await async_client.get(f"/dataset/{dataset}/{version}")
    assert version_resp.json()["data"]["status"] == "pending"

    asset_resp = await async_client.get(f"/asset/{asset_id}")
    assert asset_resp.json()["data"]["status"] == "pending"

    # At this point there should be a bunch of tasks rows for the default
    # asset, though we've mocked out the actual creation of Batch jobs.
    # That's fine, we're going to update the task rows via the task status
    # endpoint the same way the Batch tasks would (though via the test
    # client instead of curl).

    # Verify the existence of the tasks, and that they each have only the
    # initial changelog with status "pending"
    get_resp = await async_client.get(f"/asset/{asset_id}/tasks")
    existing_tasks = get_resp.json()["data"]

    assert len(existing_tasks) == 8
    for task in existing_tasks:
        assert len(task["change_log"]) == 1
        assert task["change_log"][0]["status"] == "pending"

    # Arbitrarily choose a task and add a changelog indicating the task
    # failed.
    sample_task_id = existing_tasks[0]["task_id"]
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
    patch_resp = await async_client.patch(f"/task/{sample_task_id}", json=patch_payload)
    assert patch_resp.json()["status"] == "success"

    # Verify that the asset and version have been changed to state "failed"
    version_resp = await async_client.get(f"/dataset/{dataset}/{version}")
    assert version_resp.json()["data"]["status"] == "failed"


@pytest.mark.asyncio
async def test_fail_create_task(async_client):
    change_log = [
        {
            "date_time": "2020-06-25 14:30:00",
            "status": "success",
            "message": "All good",
            "detail": "None",
        }
    ]

    asset_id = str(generate_uuid())
    task_id = str(generate_uuid())
    response = await async_client.put(
        f"/task/{task_id}", json={"asset_id": asset_id, "change_log": change_log}
    )
    assert response.status_code == 400
    assert response.json()["status"] == "failed"
    assert response.json()["message"] == f"Asset {asset_id} does not exist."

    response = await async_client.get(f"/task/{task_id}")
    assert response.status_code == 404
    assert response.json()["status"] == "failed"
    assert response.json()["message"] == f"Task with task_id {task_id} does not exist."
