import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_query_dataset_without_api_key(
    async_client: AsyncClient, generic_vector_source_version_async
):
    dataset_name, version_name, version_metadata = generic_vector_source_version_async

    response = await async_client.get(
        f"/dataset/{dataset_name}/{version_name}/query?sql=select * from data"
    )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_query_dataset_with_api_key(
    async_client: AsyncClient, generic_vector_source_version_async, apikey
):
    api_key, origin = apikey
    dataset_name, version_name, version_metadata = generic_vector_source_version_async

    headers = {"origin": origin, "x-api-key": api_key}

    response = await async_client.get(
        f"/dataset/{dataset_name}/{version_name}/query?sql=select count(*) as count from data",
        headers=headers,
    )

    assert response.status_code == 200
    assert response.json()["data"][0]["count"] == 1
