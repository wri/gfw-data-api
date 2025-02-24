import uuid
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from httpx import AsyncClient

from app.routes.datamart.land import _get_resource_id


@pytest.mark.asyncio
async def test_get_tree_cover_loss_by_drivers_not_found(
    geostore,
    apikey,
    async_client: AsyncClient,
):
    with patch(
        "app.routes.datamart.land._get_resource", side_effect=HTTPException(404)
    ) as mock_get_resources:
        api_key, payload = apikey
        origin = payload["domains"][0]

        headers = {"origin": origin}
        params = {"x-api-key": api_key, "geostore_id": geostore, "canopy_cover": 30}

        response = await async_client.get(
            "/v0/land/tree-cover-loss-by-driver", headers=headers, params=params
        )

        assert response.status_code == 404

        resource_id = _get_resource_id(geostore, 30)
        mock_get_resources.assert_awaited_with(resource_id)


@pytest.mark.asyncio
async def test_get_tree_cover_loss_by_drivers_found(
    geostore,
    apikey,
    async_client: AsyncClient,
):
    with patch(
        "app.routes.datamart.land._get_resource", return_value=None
    ) as mock_get_resources:
        api_key, payload = apikey
        origin = payload["domains"][0]

        headers = {"origin": origin}
        params = {"x-api-key": api_key, "geostore_id": geostore, "canopy_cover": 30}
        resource_id = _get_resource_id(geostore, 30)

        response = await async_client.get(
            "/v0/land/tree-cover-loss-by-driver", headers=headers, params=params
        )

        assert response.status_code == 200
        assert (
            f"/v0/land/tree-cover-loss-by-driver/{resource_id}"
            in response.json()["data"]["link"]
        )
        mock_get_resources.assert_awaited_with(resource_id)


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
    with (
        patch("app.routes.datamart.land._save_pending_result") as mock_pending_result,
        patch(
            "app.routes.datamart.land.compute_tree_cover_loss_by_driver"
        ) as mock_compute_result,
    ):
        response = await async_client.post(
            "/v0/land/tree-cover-loss-by-driver", headers=headers, json=payload
        )
        assert response.status_code == 202

        body = response.json()
        assert body["status"] == "success"
        assert "/v0/land/tree-cover-loss-by-driver/" in body["data"]["link"]

        resource_id = body["data"]["link"].split("/")[-1]
        try:
            resource_id = uuid.UUID(resource_id)
            assert True
        except ValueError:
            assert False

        mock_pending_result.assert_awaited_with(resource_id)
        mock_compute_result.assert_awaited_with(resource_id, uuid.UUID(geostore), 30)


@pytest.mark.asyncio
async def test_get_tree_cover_loss_by_drivers_after_create_with_retry(
    geostore,
    apikey,
    async_client: AsyncClient,
):
    api_key, payload = apikey
    origin = payload["domains"][0]

    headers = {"origin": origin, "x-api-key": api_key}
    with patch(
        "app.routes.datamart.land._get_resource", return_value={"status": "pending"}
    ):
        resource_id = _get_resource_id(geostore, 30)
        response = await async_client.get(
            f"/v0/land/tree-cover-loss-by-driver/{resource_id}", headers=headers
        )

        assert response.status_code == 200
        assert int(response.headers["Retry-After"]) == 1
        assert response.json()["data"]["status"] == "pending"


@pytest.mark.asyncio
async def test_get_tree_cover_loss_by_drivers_after_create_saved(
    geostore,
    apikey,
    async_client: AsyncClient,
):
    api_key, payload = apikey
    origin = payload["domains"][0]

    headers = {"origin": origin, "x-api-key": api_key}

    with patch("app.routes.datamart.land._get_resource", return_value=MOCK_RESULT):
        resource_id = _get_resource_id(geostore, 30)
        response = await async_client.get(
            f"/v0/land/tree-cover-loss-by-driver/{resource_id}", headers=headers
        )

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
                {
                    "dataset": "wri_google_tree_cover_loss_by_drivers",
                    "version": "v1.11",
                },
                {"dataset": "umd_tree_cover_density_2000", "version": "v1.11"},
            ]
        }


MOCK_RESULT = {
    "status": "saved",
    "treeCoverLossByDriver": {
        "Permanent agriculture": 10,
        "Hard commodities": 12,
        "Shifting cultivation": 7,
        "Forest management": 93.4,
        "Wildfires": 42,
        "Settlements and infrastructure": 13.562,
        "Other natural disturbances": 6,
    },
    "metadata": {
        "sources": [
            {"dataset": "umd_tree_cover_loss", "version": "v1.11"},
            {"dataset": "wri_google_tree_cover_loss_by_drivers", "version": "v1.11"},
            {"dataset": "umd_tree_cover_density_2000", "version": "v1.11"},
        ]
    },
}
