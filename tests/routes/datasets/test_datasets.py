from unittest.mock import patch

import pytest
from httpx import AsyncClient

from app.application import ContextEngine, db
from tests.utils import create_default_asset

payload = {
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
        "added_date": "2020-06-25",
        "why_added": "string",
        "other": "string",
        "learn_more": "string",
    }
}


@pytest.mark.asyncio
async def test_datasets(async_client: AsyncClient):
    """Basic test to check if empty data api response as expected."""

    dataset = "test"

    response = await async_client.get("/datasets")
    assert response.status_code == 200
    assert response.json() == {"data": [], "status": "success"}

    response = await async_client.put(f"/dataset/{dataset}", json=payload)
    assert response.status_code == 201
    assert response.json()["data"]["metadata"] == payload["metadata"]

    response = await async_client.get("/datasets")
    assert response.status_code == 200
    assert len(response.json()["data"]) == 1
    assert response.json()["data"][0]["metadata"] == payload["metadata"]

    response = await async_client.get(f"/dataset/{dataset}")
    assert response.status_code == 200
    assert response.json()["data"]["metadata"] == payload["metadata"]

    async with ContextEngine("READ"):
        rows = await db.all(
            f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{dataset}';"
        )

    assert len(rows) == 1

    new_payload = {"metadata": {"title": "New Title"}}
    response = await async_client.patch(f"/dataset/{dataset}", json=new_payload)
    assert response.status_code == 200
    assert response.json()["data"]["metadata"] != payload["metadata"]
    assert response.json()["data"]["metadata"]["title"] == "New Title"
    assert response.json()["data"]["metadata"]["subtitle"] == "string"

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
async def test_dataset_delete_protection(async_client: AsyncClient):
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
async def test_put_latest(async_client: AsyncClient):
    response = await async_client.put("/dataset/latest")
    assert response.status_code == 400
    assert response.json()["message"] == "Name `latest` is reserved for versions only."
