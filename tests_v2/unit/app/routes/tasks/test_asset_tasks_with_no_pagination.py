import pytest
from httpx import AsyncClient

from app.models.pydantic.tasks import TasksResponse


@pytest.mark.asyncio
async def test_get_asset_tasks_returns_tasks_response(
    async_client: AsyncClient, generic_vector_source_version
) -> None:

    dataset_name, dataset_version, _ = generic_vector_source_version
    version_resp = await async_client.get(
        f"/dataset/{dataset_name}/{dataset_version}/assets"
    )
    asset_id = version_resp.json()["data"][0]["asset_id"]
    resp = await async_client.get(f"/asset/{asset_id}/tasks")

    assert TasksResponse(**resp.json())
