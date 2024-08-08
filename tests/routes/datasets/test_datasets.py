from unittest.mock import patch

import pytest
from tests import MANAGER_1, MANAGER_2, USER_1
from tests.conftest import client_with_mocks
from tests.utils import create_default_asset, dataset_metadata

payload = {"metadata": dataset_metadata}


@pytest.mark.asyncio
async def test_datasets_basic_create_delete(db_clean, async_client):
    """Test routes for getting, creating, and deleting datasets"""
    dataset = "test"

    # No datasets
    response = await async_client.get("/datasets")
    assert response.status_code == 200
    assert response.json() == {"data": [], "status": "success"}

    # Create a dataset
    response = await async_client.put(f"/dataset/{dataset}", json=payload)
    assert response.status_code == 201
    assert response.json()["data"]["metadata"]["title"] == payload["metadata"]["title"]
    assert (
        response.json()["data"]["metadata"]["source"] == payload["metadata"]["source"]
    )
    assert (
        response.json()["data"]["metadata"]["data_language"]
        == payload["metadata"]["data_language"]
    )

    # Verify it has been created
    response = await async_client.get("/datasets")
    assert response.status_code == 200
    assert len(response.json()["data"]) == 1
    assert response.json()["data"][0]["dataset"] == dataset

    # Delete it
    response = await async_client.delete(f"/dataset/{dataset}")
    assert response.status_code == 200
    assert response.json()["data"]["dataset"] == "test"

    # Verify it is gone
    response = await async_client.get("/datasets")
    assert response.status_code == 200
    assert response.json() == {"data": [], "status": "success"}


@pytest.mark.asyncio
async def test_datasets_ownership_no_mortals(db_clean):
    """Make sure permissions function correctly on dataset routes."""
    dataset = "test"

    # Try and fail to make a dataset as a normal user
    async with client_with_mocks(False, False, True) as test_client:
        response = await test_client.put(f"/dataset/{dataset}", json=payload)
        assert response.status_code == 401


@pytest.mark.asyncio
async def test_datasets_ownership(db_clean):
    """Make sure permissions function correctly on dataset routes."""
    dataset = "test"

    # Now make a dataset as a manager, which should become its owner
    async with client_with_mocks(False, MANAGER_1, False) as test_client:
        response = await test_client.put(f"/dataset/{dataset}", json=payload)
        assert response.status_code == 201

        response = await test_client.get(f"/dataset/{dataset}")
        assert response.status_code == 200
        # TODO: Verify owner_id. Not included in response, so this doesn't work:
        # assert len(response.json()["data"]["owner_id"]) == MANAGER_1.id

    # Try to delete it as not-its-owner (but still a manager)
    async with client_with_mocks(False, MANAGER_2, False) as test_client:
        response = await test_client.delete(f"/dataset/{dataset}")
        assert response.status_code == 401

    # Try to change owner to a non-admin, non-manager
    with patch("app.routes.datasets.dataset.get_rw_user", return_value=USER_1):
        async with client_with_mocks(False, MANAGER_1, False) as test_client:
            new_payload = {
                "owner_id": USER_1.id,
            }
            response = await test_client.patch(f"/dataset/{dataset}", json=new_payload)
            assert response.status_code == 400

    # Change owner (as owner) to another manager
    with patch("app.routes.datasets.dataset.get_rw_user", return_value=MANAGER_2):
        async with client_with_mocks(False, MANAGER_1, False) as test_client:
            new_payload = {
                "owner_id": MANAGER_2.id,
            }
            response = await test_client.patch(f"/dataset/{dataset}", json=new_payload)
            assert response.status_code == 200
            # TODO: Verify new owner_id. Not included in response, so this doesn't work:
            # response = await async_client.get(f"/dataset/{dataset}")
            # assert len(response.json()["data"]["owner_id"]) == MANAGER_2.id

    # Delete it as the new owner
    async with client_with_mocks(False, MANAGER_2, False) as test_client:
        response = await test_client.delete(f"/dataset/{dataset}")
        assert response.status_code == 200


@pytest.mark.asyncio
async def test_dataset_delete_protection(async_client):
    dataset = "test"
    version = "v20200626"

    await create_default_asset(
        dataset, version, async_client=async_client, execute_batch_jobs=False
    )

    with patch("fastapi.BackgroundTasks.add_task", return_value=None) as mocked_task:

        # You should not be able to delete datasets while there are still versions
        # You will need to delete the version first
        response = await async_client.delete(f"/dataset/{dataset}")
        assert response.status_code == 409

        response = await async_client.delete(f"/dataset/{dataset}/{version}")
        assert response.status_code == 200

        response = await async_client.delete(f"/dataset/{dataset}")
        assert response.status_code == 200
        assert mocked_task.called


@pytest.mark.asyncio
async def test_put_latest(async_client):
    response = await async_client.put("/dataset/latest")
    assert response.status_code == 400
    assert response.json()["message"] == "Name `latest` is reserved for versions only."
