import json
import uuid
from unittest.mock import patch

from tests.routes import create_default_asset


def test_tasks_success(client, db):
    """Verify that all tasks succeeding -> 'saved' status for default
    asset/version.

    After that, make sure adding auxiliary assets doesn't affect version
    status.
    """
    # Add a dataset, version, and default asset
    dataset = "test"
    version = "v20200626"

    def generate_uuid(*args, **kwargs):
        return uuid.uuid4()

    with patch("app.tasks.batch.submit_batch_job", side_effect=generate_uuid):
        asset = create_default_asset(client, dataset, version)
    asset_id = asset["asset_id"]

    # Verify that the asset and version are in state "pending"
    version_resp = client.get(f"/meta/{dataset}/{version}")
    assert version_resp.json()["data"]["status"] == "pending"

    asset_resp = client.get(f"/meta/{dataset}/{version}/assets/{asset_id}")
    assert asset_resp.json()["data"]["status"] == "pending"

    # At this point there should be a bunch of tasks rows for the default
    # asset, though we've mocked out the actual creation of Batch jobs.
    # That's fine, we're going to update the task rows via the task status
    # endpoint the same way the Batch tasks would (though via the test
    # client instead of curl).

    # Verify the existence of the tasks, and that they each have only the
    # initial changelog with status "pending"
    existing_tasks = client.get(f"/tasks/assets/{asset_id}").json()["data"]
    assert len(existing_tasks) == 7
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
    patch_resp = client.patch(f"/tasks/{sample_task_id}", json=patch_payload)
    assert patch_resp.json()["status"] == "success"

    # Verify the task has two changelogs now.
    get_resp = client.get(f"/tasks/{sample_task_id}")
    assert len(get_resp.json()["data"]["change_log"]) == 2

    # Verify that the asset and version are still in state "pending"
    version_resp = client.get(f"/meta/{dataset}/{version}")
    assert version_resp.json()["data"]["status"] == "pending"

    asset_resp = client.get(f"/meta/{dataset}/{version}/assets/{asset_id}")
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
        patch_resp = client.patch(f"/tasks/{task['task_id']}", json=patch_payload)
        assert patch_resp.json()["status"] == "success"

    version_resp = client.get(f"/meta/{dataset}/{version}")
    assert version_resp.json()["data"]["status"] == "saved"

    asset_resp = client.get(f"/meta/{dataset}/{version}/assets/{asset_id}")
    assert asset_resp.json()["data"]["status"] == "saved"

    # The following will fail until creation of auxiliary assets is working

    field_payload = {
        "metadata": {
            "fields": [
                {"field_name": "test", "field_type": "numeric", "is_feature_info": True}
            ]
        }
    }

    asset_resp = client.patch(
        f"/meta/{dataset}/{version}/assets/{asset_id}", json=json.dumps(field_payload)
    )
    print(asset_resp.json())
    assert asset_resp.json()["status"] == "success"

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
        },
    }
    create_asset_resp = client.post(
        f"/meta/{dataset}/{version}/assets", json=asset_payload
    )
    print(json.dumps(create_asset_resp.json(), indent=2))
    assert create_asset_resp.json()["status"] == "success"
    asset_id = create_asset_resp.json()["data"]["asset_id"]

    # Verify there are two assets now
    get_resp = client.get(f"/meta/{dataset}/{version}/assets")
    assert len(get_resp.json()["data"]) == 2

    # Verify the existence of tasks for the new asset
    non_default_tasks = client.get(f"/tasks/assets/{asset_id}").json()["data"]
    assert len(existing_tasks) == 7
    for task in non_default_tasks:
        assert len(task["change_log"]) == 1
        assert task["change_log"][0]["status"] == "pending"

    # Arbitrarily choose a task and add a changelog with status "failed"
    sample_task_id = existing_tasks[0]["task_id"]
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
    patch_resp = client.patch(f"/tasks/{sample_task_id}", json=patch_payload)
    assert patch_resp.json()["status"] == "success"

    # Verify the asset status is now "failed"
    get_resp = client.get(f"/meta/{dataset}/{version}/assets/{asset_id}")
    assert get_resp.json()["data"]["status"] == "failed"

    # ... but that the version status is still "saved"
    get_resp = client.get(f"/meta/{dataset}/{version}")
    assert get_resp.json()["data"]["status"] == "saved"


def test_tasks_failure(client, db):
    """Verify that failing tasks result in failed asset/version."""
    # Add a dataset, version, and default asset
    dataset = "test"
    version = "v20200626"

    def generate_uuid(*args, **kwargs):
        return uuid.uuid4()

    with patch("app.tasks.batch.submit_batch_job", side_effect=generate_uuid):
        asset = create_default_asset(client, dataset, version)
    asset_id = asset["asset_id"]

    # Verify that the asset and version are in state "pending"
    version_resp = client.get(f"/meta/{dataset}/{version}")
    assert version_resp.json()["data"]["status"] == "pending"

    asset_resp = client.get(f"/meta/{dataset}/{version}/assets/{asset_id}")
    assert asset_resp.json()["data"]["status"] == "pending"

    # At this point there should be a bunch of tasks rows for the default
    # asset, though we've mocked out the actual creation of Batch jobs.
    # That's fine, we're going to update the task rows via the task status
    # endpoint the same way the Batch tasks would (though via the test
    # client instead of curl).

    # Verify the existence of the tasks, and that they each have only the
    # initial changelog with status "pending"
    existing_tasks = client.get(f"/tasks/assets/{asset_id}").json()["data"]

    assert len(existing_tasks) == 7
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
    patch_resp = client.patch(f"/tasks/{sample_task_id}", json=patch_payload)
    assert patch_resp.json()["status"] == "success"

    # Verify that the asset and version have been changed to state "failed"
    version_resp = client.get(f"/meta/{dataset}/{version}")
    assert version_resp.json()["data"]["status"] == "failed"
