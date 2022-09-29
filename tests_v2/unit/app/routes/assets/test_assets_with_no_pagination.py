import pytest
from httpx import AsyncClient

from app.models.pydantic.assets import AssetsResponse


@pytest.mark.asyncio
async def test_get_assets_returns_assets_response(async_client: AsyncClient) -> None:
    resp = await async_client.get("/assets")
    assert AssetsResponse(**resp.json())
