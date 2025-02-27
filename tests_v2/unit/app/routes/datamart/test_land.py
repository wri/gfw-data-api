import uuid

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
        "/v0/land/tree-cover-loss-by-driver", headers=headers, params=params
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
        "/v0/land/tree-cover-loss-by-driver", headers=headers, json=payload
    )

    print(response.json())
    assert response.status_code == 202

    body = response.json()
    assert body["status"] == "success"
    assert "/v0/land/tree-cover-loss-by-driver/" in body["data"]["link"]

    resource_id = body["data"]["link"].split("/")[-1]
    try:
        uuid.UUID(resource_id)
        assert True
    except ValueError:
        assert False


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
        "/v0/land/tree-cover-loss-by-driver", headers=headers, json=payload
    )

    assert response.status_code == 202

    body = response.json()
    assert body["status"] == "success"

    link = body["data"]["link"]
    retries = 0
    while retries < 3:
        response = await async_client.get(link, headers=headers)

        if "Retry-After" in response.headers:
            assert response.status_code == 200
            assert int(response.headers["Retry-After"]) == 1
            assert response.json()["data"]["status"] == "pending"
            retries += 1
        else:
            break

    assert not retries == 4
    assert response.status_code == 200
    assert "Retry-After" not in response.headers

    data = response.json()["data"]
    assert data["treeCoverLossByDriver"] == {
        "Permanent agriculture": 10,
        "Hard commodities": 12,
        "Shifting cultivation": 7,
        "Forest management": 93.4,
        "Wildfires": 42,
        "Settlements and infrastructure": 13.562,
        "Other natural disturbances": 6,
    }
    assert data["metadata"] == {
        "sources": [
            {"dataset": "umd_tree_cover_loss", "version": "v1.11"},
            {"dataset": "wri_google_tree_cover_loss_by_drivers", "version": "v1.11"},
            {"dataset": "umd_tree_cover_density_2000", "version": "v1.11"},
        ]
    }

    # now we should also see the resource when looking with the same params
    params = {"x-api-key": api_key, "geostore_id": geostore, "canopy_cover": 30}
    response = await async_client.get(
        "/v0/land/tree-cover-loss-by-driver", headers=headers, params=params
    )
    assert response.status_code == 200
    assert response.json()["data"]["link"] == link
