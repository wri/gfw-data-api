import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_get_tree_cover_loss_by_drivers_not_found(
    geostore,
    apikey,
    async_client: AsyncClient,
):
    api_key, payload = apikey
    origin = payload["domains"][0]

    headers = {"origin": origin}
    params = {"x-api-key": api_key, "geostore_id": geostore, "canopy_cover": 30}

    response = await async_client.get(
        "/v0/land/tree-cover-loss-by-drivers", headers=headers, params=params
    )

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_post_tree_cover_loss_by_drivers(
    geostore,
    apikey,
    async_client: AsyncClient,
):
    api_key, payload = apikey
    origin = payload["domains"][0]

    headers = {"origin": origin, "x-api-key": api_key}
    payload = {"geostore_id": geostore, "canopy_cover": 30}

    response = await async_client.post(
        "/v0/land/tree-cover-loss-by-drivers", headers=headers, json=payload
    )

    assert response.status_code == 202

    body = response.json()
    assert body["status"] == "success"
    assert body["data"]["link"].contains("/v0/land/tree-cover-loss-by-drivers/")
    # assert contains_valid_uuid(body["data"]["link"])


@pytest.mark.asyncio
async def test_get_tree_cover_loss_by_drivers_after_create(
    geostore,
    apikey,
    async_client: AsyncClient,
):
    api_key, payload = apikey
    origin = payload["domains"][0]

    headers = {"origin": origin, "x-api-key": api_key}
    payload = {"geostore_id": geostore, "canopy_cover": 30}

    response = await async_client.post(
        "/v0/land/tree-cover-loss-by-drivers", headers=headers, json=payload
    )

    assert response.status_code == 202

    body = response.json()
    assert body["status"] == "success"

    link = body["data"]["link"]
    response = await async_client.get(link, headers=headers)

    assert response.status_code == 200
    assert "Retry-After" in response.headers
    assert response.headers["Retry-After"] == 1

    response = await async_client.get(link, headers=headers)

    assert response.status_code == 200
    assert "Retry-After" not in response.headers

    data = response.json()["data"]
    assert data == {}
