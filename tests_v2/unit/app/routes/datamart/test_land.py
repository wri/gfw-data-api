import uuid
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from httpx import AsyncClient

from app.models.enum.geostore import GeostoreOrigin
from app.models.pydantic.datamart import TreeCoverLossByDriver
from app.routes.datamart.land import _get_resource_id
from app.tasks.datamart.land import (
    DEFAULT_LAND_DATASET_VERSIONS,
    compute_tree_cover_loss_by_driver,
)
from app.utils.geostore import get_geostore


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
            "/v0/land/tree_cover_loss_by_driver", headers=headers, params=params
        )

        assert response.status_code == 404

        resource_id = _get_resource_id(
            "tree_cover_loss_by_driver", geostore, 30, DEFAULT_LAND_DATASET_VERSIONS
        )
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
        resource_id = _get_resource_id(
            "tree_cover_loss_by_driver", geostore, 30, DEFAULT_LAND_DATASET_VERSIONS
        )

        response = await async_client.get(
            "/v0/land/tree_cover_loss_by_driver", headers=headers, params=params
        )

        assert response.status_code == 200
        assert (
            f"/v0/land/tree_cover_loss_by_driver/{resource_id}"
            in response.json()["data"]["link"]
        )
        mock_get_resources.assert_awaited_with(resource_id)


@pytest.mark.asyncio
async def test_get_tree_cover_loss_by_drivers_with_overrides(
    geostore,
    apikey,
    async_client: AsyncClient,
):
    with (
        patch(
            "app.routes.datamart.land._get_resource", return_value=None
        ) as mock_get_resources,
        patch(
            "app.routes.datamart.land._get_resource_id", side_effect=_get_resource_id
        ) as mock_get_resource_id,
    ):
        api_key, payload = apikey
        origin = payload["domains"][0]

        headers = {"origin": origin}
        params = {"x-api-key": api_key, "geostore_id": geostore, "canopy_cover": 30}
        resource_id = _get_resource_id(
            "tree_cover_loss_by_driver",
            geostore,
            30,
            {
                "umd_tree_cover_loss": "v1.8",
                "tsc_tree_cover_loss_drivers": "v2023",
                "umd_tree_cover_density_2000": "v1.6",
            },
        )

        response = await async_client.get(
            "/v0/land/tree_cover_loss_by_driver?x-api-key={api_key}&geostore_id={geostore_id}&canopy_cover=30&dataset_version[umd_tree_cover_loss]=v1.8&dataset_version[umd_tree_cover_density_2000]=v1.6",
            headers=headers,
            params=params,
        )

        assert response.status_code == 200
        mock_get_resource_id.assert_called_with(
            "tree_cover_loss_by_driver",
            uuid.UUID(geostore),
            30,
            {
                "umd_tree_cover_loss": "v1.8",
                "tsc_tree_cover_loss_drivers": "v2023",
                "umd_tree_cover_density_2000": "v1.6",
            },
        )
        assert (
            f"/v0/land/tree_cover_loss_by_driver/{resource_id}"
            in response.json()["data"]["link"]
        )
        mock_get_resources.assert_awaited_with(resource_id)


@pytest.mark.asyncio
async def test_get_tree_cover_loss_by_drivers_with_malformed_overrides(
    geostore,
    apikey,
    async_client: AsyncClient,
):
    api_key, payload = apikey
    origin = payload["domains"][0]

    headers = {"origin": origin}
    params = {"x-api-key": api_key, "geostore_id": geostore, "canopy_cover": 30}

    response = await async_client.get(
        "/v0/land/tree_cover_loss_by_driver?x-api-key={api_key}&geostore_id={geostore_id}&canopy_cover=30&dataset_version[umd_tree_cover_loss]]=v1.8&dataset_version[umd_tree_cover_density_2000]=v1.6",
        headers=headers,
        params=params,
    )

    assert response.status_code == 422
    assert (
        response.json()["message"]
        == "Could not parse the following malformed dataset_version parameters: ['dataset_version[umd_tree_cover_loss]]']"
    )


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
        patch(
            "app.routes.datamart.land._save_pending_resource"
        ) as mock_pending_resource,
        patch(
            "app.routes.datamart.land.compute_tree_cover_loss_by_driver"
        ) as mock_compute_result,
    ):
        response = await async_client.post(
            "/v0/land/tree_cover_loss_by_driver", headers=headers, json=payload
        )
        assert response.status_code == 202

        body = response.json()
        assert body["status"] == "success"
        assert "/v0/land/tree_cover_loss_by_driver/" in body["data"]["link"]

        resource_id = body["data"]["link"].split("/")[-1]
        try:
            resource_id = uuid.UUID(resource_id)
            assert True
        except ValueError:
            assert False

        mock_pending_resource.assert_awaited_with(resource_id)
        mock_compute_result.assert_awaited_with(
            resource_id, uuid.UUID(geostore), 30, DEFAULT_LAND_DATASET_VERSIONS
        )


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
        resource_id = _get_resource_id(
            "tree_cover_loss_by_driver", geostore, 30, DEFAULT_LAND_DATASET_VERSIONS
        )
        response = await async_client.get(
            f"/v0/land/tree_cover_loss_by_driver/{resource_id}", headers=headers
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
    MOCK_RESOURCE["metadata"]["geostore_id"] = geostore

    with patch("app.routes.datamart.land._get_resource", return_value=MOCK_RESOURCE):
        resource_id = _get_resource_id(
            "tree_cover_loss_by_driver", geostore, 30, DEFAULT_LAND_DATASET_VERSIONS
        )
        response = await async_client.get(
            f"/v0/land/tree_cover_loss_by_driver/{resource_id}", headers=headers
        )

        assert response.status_code == 200
        assert "Retry-After" not in response.headers

        data = response.json()["data"]
        assert data == MOCK_RESOURCE


@pytest.mark.asyncio
async def test_compute_tree_cover_loss_by_driver(geostore):
    with (
        patch(
            "app.tasks.datamart.land._query_dataset_json", return_value=MOCK_RESULT
        ) as mock_query_dataset_json,
        patch("app.tasks.datamart.land._write_resource") as mock_write_result,
    ):
        resource_id = _get_resource_id(
            "tree_cover_loss_by_driver", geostore, 30, DEFAULT_LAND_DATASET_VERSIONS
        )
        geostore_common = await get_geostore(geostore, GeostoreOrigin.rw)

        await compute_tree_cover_loss_by_driver(
            resource_id, geostore, 30, DEFAULT_LAND_DATASET_VERSIONS
        )

        mock_query_dataset_json.assert_awaited_once_with(
            "umd_tree_cover_loss",
            "v1.11",
            "SELECT SUM(area__ha) FROM data WHERE umd_tree_cover_density_2000__threshold >= 30 GROUP BY tsc_tree_cover_loss_drivers__driver",
            geostore_common,
            DEFAULT_LAND_DATASET_VERSIONS,
        )

        MOCK_RESOURCE["metadata"]["geostore_id"] = geostore
        mock_write_result.assert_awaited_once_with(
            resource_id, TreeCoverLossByDriver.parse_obj(MOCK_RESOURCE)
        )


@pytest.mark.asyncio
async def test_compute_tree_cover_loss_by_driver_error(geostore):
    with (
        patch(
            "app.tasks.datamart.land._query_dataset_json",
            side_effect=HTTPException(status_code=500, detail="error"),
        ) as mock_query_dataset_json,
        patch("app.tasks.datamart.land._write_error") as mock_write_error,
    ):
        resource_id = _get_resource_id(
            "tree_cover_loss_by_driver", geostore, 30, DEFAULT_LAND_DATASET_VERSIONS
        )
        geostore_common = await get_geostore(geostore, GeostoreOrigin.rw)

        await compute_tree_cover_loss_by_driver(
            resource_id, geostore, 30, DEFAULT_LAND_DATASET_VERSIONS
        )

        mock_query_dataset_json.assert_awaited_once_with(
            "umd_tree_cover_loss",
            "v1.11",
            "SELECT SUM(area__ha) FROM data WHERE umd_tree_cover_density_2000__threshold >= 30 GROUP BY tsc_tree_cover_loss_drivers__driver",
            geostore_common,
            DEFAULT_LAND_DATASET_VERSIONS,
        )
        mock_write_error.assert_awaited_once_with(resource_id, "500: error")


MOCK_RESULT = [
    {
        "tsc_tree_cover_loss_drivers__driver": "Permanent agriculture",
        "area__ha": 10,
    },
    {
        "tsc_tree_cover_loss_drivers__driver": "Hard commodities",
        "area__ha": 12,
    },
    {
        "tsc_tree_cover_loss_drivers__driver": "Shifting cultivation",
        "area__ha": 7,
    },
    {
        "tsc_tree_cover_loss_drivers__driver": "Forest management",
        "area__ha": 93.4,
    },
    {
        "tsc_tree_cover_loss_drivers__driver": "Wildfires",
        "area__ha": 42,
    },
    {
        "tsc_tree_cover_loss_drivers__driver": "Settlements and infrastructure",
        "area__ha": 13.562,
    },
    {
        "tsc_tree_cover_loss_drivers__driver": "Other natural disturbances",
        "area__ha": 6,
    },
]


MOCK_RESOURCE = {
    "status": "saved",
    "details": None,
    "tree_cover_loss_by_driver": {
        "Permanent agriculture": 10.0,
        "Hard commodities": 12.0,
        "Shifting cultivation": 7.0,
        "Forest management": 93.4,
        "Wildfires": 42.0,
        "Settlements and infrastructure": 13.562,
        "Other natural disturbances": 6.0,
    },
    "metadata": {
        "geostore_id": "",
        "canopy_cover": 30,
        "sources": [
            {"dataset": "umd_tree_cover_loss", "version": "v1.11"},
            {"dataset": "tsc_tree_cover_loss_drivers", "version": "v2023"},
            {"dataset": "umd_tree_cover_density_2000", "version": "v1.8"},
        ],
    },
}
