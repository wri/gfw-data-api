import pytest
from _pytest.monkeypatch import MonkeyPatch
from httpx import AsyncClient

from app.routes.datasets import queries
from tests_v2.utils import invoke_lambda_mocked


@pytest.mark.skip("Temporarily skip until we require API keys")
@pytest.mark.asyncio
async def test_query_dataset_without_api_key(
    generic_vector_source_version, async_client: AsyncClient
):
    dataset_name, version_name, _ = generic_vector_source_version

    response = await async_client.get(
        f"/dataset/{dataset_name}/{version_name}/query?sql=select * from data"
    )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_query_dataset_with_api_key(
    generic_vector_source_version, apikey, async_client: AsyncClient
):
    dataset_name, version_name, _ = generic_vector_source_version
    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]

    headers = {"origin": origin, "x-api-key": api_key}

    response = await async_client.get(
        f"/dataset/{dataset_name}/{version_name}/query?sql=select count(*) as count from data",
        headers=headers,
    )

    assert response.status_code == 200
    assert response.json()["data"][0]["count"] == 1


@pytest.mark.asyncio
async def test_query_dataset_with_unrestricted_api_key(
    generic_vector_source_version, apikey_unrestricted, async_client: AsyncClient
):
    dataset_name, version_name, _ = generic_vector_source_version
    api_key, payload = apikey_unrestricted

    # no need to add origin here, since api key in unrestricted
    headers = {"x-api-key": api_key}

    response = await async_client.get(
        f"/dataset/{dataset_name}/{version_name}/query?sql=select count(*) as count from data",
        headers=headers,
    )

    assert response.status_code == 200
    assert response.json()["data"][0]["count"] == 1


@pytest.mark.asyncio
async def test_query_dataset_raster(
    generic_raster_version,
    apikey,
    geostore,
    monkeypatch: MonkeyPatch,
    async_client: AsyncClient,
):
    dataset_name, version_name, _ = generic_raster_version
    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]

    headers = {"origin": origin, "x-api-key": api_key}

    monkeypatch.setattr(queries, "invoke_lambda", invoke_lambda_mocked)

    response = await async_client.get(
        f"/dataset/{dataset_name}/{version_name}/query?sql=select count(*) from data&geostore_id={geostore}",
        headers=headers,
    )

    assert response.status_code == 200
    assert response.json()["status"] == "success"


@pytest.mark.asyncio
async def test_query_dataset_raster_geostore_huge(
    generic_raster_version,
    apikey,
    geostore_huge,
    async_client: AsyncClient,
):
    dataset_name, version_name, _ = generic_raster_version
    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]

    headers = {"origin": origin, "x-api-key": api_key}

    response = await async_client.get(
        f"/dataset/{dataset_name}/{version_name}/query?sql=select count(*) from data&geostore_id={geostore_huge}",
        headers=headers,
    )

    assert response.status_code == 400
