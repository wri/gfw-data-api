import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_geoencoder_no_version(async_client: AsyncClient) -> None:
    params = {"country": "Canada"}

    resp = await async_client.get("/thematic/geoencode", params=params)

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_geoencoder_fake_country_no_matches(async_client: AsyncClient) -> None:

    params = {"admin_version": "4.1", "country": "Canadiastan"}

    resp = await async_client.get("/thematic/geoencode", params=params)

    assert resp.status_code == 200
    assert resp.status_code == {
        "status": "success",
        "data": {
            "adminVersion": "4.1",
            "matches": []
        }
    }
