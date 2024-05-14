from unittest.mock import patch

import pytest

from app.application import ContextEngine, db
from app.authentication.token import get_manager
from app.models.pydantic.authentication import User
from tests import MANAGER, NEW_OWNER, get_manager_mocked, get_new_owner_mocked
from tests.utils import create_default_asset, dataset_metadata

payload = {"metadata": dataset_metadata}


@pytest.mark.asyncio
async def test_datasets(async_client):
    """Basic test to check if empty data api response as expected."""

    dataset = "test"

    response = await async_client.get("/datasets")
    assert response.status_code == 200
    assert response.json() == {"data": [], "status": "success"}

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

    response = await async_client.get("/datasets")
    assert response.status_code == 200
    assert len(response.json()["data"]) == 1
    assert (
        response.json()["data"][0]["metadata"]["title"] == payload["metadata"]["title"]
    )
    assert (
        response.json()["data"][0]["metadata"]["source"]
        == payload["metadata"]["source"]
    )
    assert (
        response.json()["data"][0]["metadata"]["data_language"]
        == payload["metadata"]["data_language"]
    )

    response = await async_client.get(f"/dataset/{dataset}")
    assert response.status_code == 200
    assert response.json()["data"]["metadata"]["title"] == payload["metadata"]["title"]
    assert (
        response.json()["data"]["metadata"]["source"] == payload["metadata"]["source"]
    )
    assert (
        response.json()["data"]["metadata"]["data_language"]
        == payload["metadata"]["data_language"]
    )

    async with ContextEngine("READ"):
        rows = await db.all(
            f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{dataset}';"
        )

    assert len(rows) == 1

    async with ContextEngine("READ"):
        rows = await db.all(
            f"SELECT owner_id FROM datasets WHERE dataset = '{dataset}';"
        )

    assert len(rows) == 1
    assert rows[0][0] == "mr_manager123"

    with patch("app.routes.datasets.dataset.get_rw_user", return_value=NEW_OWNER):
        new_payload = {
            "metadata": {"title": "New Title"},
            "owner_id": "new_owner_id123",
        }
        response = await async_client.patch(f"/dataset/{dataset}", json=new_payload)
        assert response.status_code == 200
        assert response.json()["data"]["metadata"] != payload["metadata"]
        assert response.json()["data"]["metadata"]["title"] == "New Title"
        assert (
            response.json()["data"]["metadata"]["data_language"]
            == payload["metadata"]["data_language"]
        )

        async with ContextEngine("READ"):
            rows = await db.all(
                f"SELECT owner_id FROM datasets WHERE dataset = '{dataset}';"
            )

        assert len(rows) == 1
        assert rows[0][0] == "new_owner_id123"

    # assign original owner back
    with patch("app.routes.datasets.dataset.get_rw_user", return_value=MANAGER):
        from app.main import app

        app.dependency_overrides[get_manager] = get_new_owner_mocked
        new_payload = {"owner_id": "mr_manager123"}
        response = await async_client.patch(f"/dataset/{dataset}", json=new_payload)
        assert response.status_code == 200

        app.dependency_overrides[get_manager] = get_manager_mocked

    bad_user = User(
        id="bad_user123",
        name="Bad User",
        email="bad_user@bad.com",
        createdAt="2021-06-13T03:18:23.000Z",
        role="USER",
        provider="local",
        providerId="1234",
        extraUserData={},
    )

    with patch("app.routes.datasets.dataset.get_rw_user", return_value=bad_user):
        new_payload = {"owner_id": "bad_user123"}
        response = await async_client.patch(f"/dataset/{dataset}", json=new_payload)
        assert response.status_code == 400

    response = await async_client.delete(f"/dataset/{dataset}")
    assert response.status_code == 200
    assert response.json()["data"]["dataset"] == "test"

    async with ContextEngine("READ"):
        rows = await db.all(
            f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{dataset}';"
        )
    assert len(rows) == 0

    response = await async_client.get("/datasets")
    assert response.status_code == 200
    assert response.json() == {"data": [], "status": "success"}


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
