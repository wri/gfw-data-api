import pytest
from httpx import AsyncClient

from tests_v2.unit.app.routes.utils import assert_jsend


@pytest.mark.asyncio
async def test_ping(async_client: AsyncClient):
    response = await async_client.get("/ping")

    assert_jsend(response.json())
    assert response.status_code == 200
    assert response.json()["data"] == "pong"
