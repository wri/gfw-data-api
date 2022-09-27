import pytest as pytest
from httpx import AsyncClient

from app.models.pydantic.assets import AssetsResponse


@pytest.mark.asyncio
async def test_get_assets_returns_assets_of_a_specific_dataset_and_version_response(
    async_client: AsyncClient, generic_vector_source_version
) -> None:
    dataset_name, dataset_version, _ = generic_vector_source_version
    resp = await async_client.get(f"/dataset/{dataset_name}/{dataset_version}/assets")
    assert AssetsResponse(**resp.json())
