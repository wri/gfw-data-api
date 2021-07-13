import pytest
from _pytest.monkeypatch import MonkeyPatch
from httpx import AsyncClient

from app.routes.datasets import queries
from tests_v2.utils import invoke_lambda_mocked


@pytest.mark.skip("Temporarily skip until we require API keys")
@pytest.mark.asyncio
async def test_analysis_without_api_key(geostore, async_client: AsyncClient):

    response = await async_client.get(f"/analysis/zonal/{geostore}")

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_analysis_with_api_key_in_header(
    geostore, apikey, async_client: AsyncClient
):
    api_key, payload = apikey

    origin = "https://" + payload["domains"][0]

    headers = {"origin": origin, "x-api-key": api_key}
    response = await async_client.get(f"/analysis/zonal/{geostore}", headers=headers)

    # this only tests if api key is correctly processed, but query will fail
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_analysis_with_api_key_as_param(
    geostore,
    apikey,
    async_client: AsyncClient,
    monkeypatch: MonkeyPatch,
):
    api_key, payload = apikey
    origin = payload["domains"][0]

    headers = {"origin": origin}
    params = {"x-api-key": api_key}

    monkeypatch.setattr(queries, "invoke_lambda", invoke_lambda_mocked)

    response = await async_client.get(
        f"/analysis/zonal/{geostore}?sum=area__ha", headers=headers, params=params
    )

    # this only tests if api key is correctly processed, but query will fail
    assert response.status_code == 200
    assert response.json()["status"] == "success"


@pytest.mark.asyncio
async def test_analysis_with_huge_geostore(
    geostore_huge, apikey, async_client: AsyncClient
):
    api_key, payload = apikey
    origin = payload["domains"][0]

    headers = {"origin": origin}
    params = {"x-api-key": api_key}
    response = await async_client.get(
        f"/analysis/zonal/{geostore_huge}?sum=area__ha", headers=headers, params=params
    )

    assert response.status_code == 400
