from io import StringIO
from unittest.mock import patch

import httpx
import pytest
from _pytest.monkeypatch import MonkeyPatch
from httpx import AsyncClient

from app.models.enum.geostore import GeostoreOrigin
from app.models.pydantic.geostore import GeostoreCommon
from app.routes.datasets import queries

TEST_SQL = "select count(*) as count from data"
PARAMS = {"sql": TEST_SQL}
GEOTIFF_PARAMS = {"tile_id": "10N_010E", "grid": "10/40000", "pixel_meaning": "test"}
TEST_DATA = [
    ("get", "json", PARAMS, 200),
    ("post", "json", PARAMS, 200),
    ("get", "csv", PARAMS, 200),
    ("post", "csv", PARAMS, 200),
    ("get", "shp", PARAMS, 501),
    ("get", "gpkg", PARAMS, 501),
    ("get", "geotiff", GEOTIFF_PARAMS, 501),
]
TEST_GEOJSON = {
    "type": "Polygon",
    "coordinates": [
        [
            [-60.590457916259766, -15.095079526355857],
            [-60.60298919677734, -15.090936030923759],
            [-60.60161590576172, -15.104774989795663],
            [-60.590457916259766, -15.095079526355857],
        ]
    ],
}
TEST_GEOSTORE_ID = "c3833748f6815d31bad47d47f147c0f0"
TEST_GEOSTORE = GeostoreCommon(
    geojson=TEST_GEOJSON, geostore_id=TEST_GEOSTORE_ID, area__ha=0, bbox=[0, 0, 0, 0]
)


@pytest.mark.parametrize("method, format, params, status_code", TEST_DATA)
@pytest.mark.asyncio
async def test_downloads(
    method,
    format,
    params,
    status_code,
    generic_vector_source_version,
    apikey,
    async_client: AsyncClient,
):
    dataset_name, version_name, _ = generic_vector_source_version
    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]
    headers = {"origin": origin, "x-api-key": api_key}
    payload = {"is_downloadable": False}

    # Proofing that the download works initially
    if method == "get":
        response = await async_client.get(
            f"/dataset/{dataset_name}/{version_name}/download/{format}",
            params=params,
            headers=headers,
        )
    else:
        response = await async_client.post(
            f"/dataset/{dataset_name}/{version_name}/download/{format}",
            json=params,
            headers=headers,
        )

    assert response.status_code == status_code

    # Set version to is_downloadable = False
    response = await async_client.patch(
        f"/dataset/{dataset_name}/{version_name}",
        json=payload,
        headers=headers,
    )

    assert response.status_code == 200

    # This should now block the download for all formats
    if method == "get":
        response = await async_client.get(
            f"/dataset/{dataset_name}/{version_name}/download/{format}",
            params=params,
            headers=headers,
        )
    else:
        response = await async_client.post(
            f"/dataset/{dataset_name}/{version_name}/download/{format}",
            json=params,
            headers=headers,
        )

    assert response.status_code == 403


@pytest.mark.asyncio()
async def test_download_vector_asset_count(
    generic_vector_source_version, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/download/csv?sql=select count(*) from mytable;",
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert response.text == '"count"\r\n1\r\n'


@pytest.mark.asyncio
async def test_download_raster_json_with_geometry(
    generic_raster_version, apikey, async_client: AsyncClient, monkeypatch: MonkeyPatch
):
    dataset_name, version_name, _ = generic_raster_version
    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]
    headers = {"origin": origin, "x-api-key": api_key}

    async def httpx_response_coroutine(*args, **kwargs) -> httpx.Response:
        return httpx.Response(status_code=200, json={"status": "success", "data": {}})

    monkeypatch.setattr(queries, "invoke_lambda", httpx_response_coroutine)

    response = await async_client.post(
        f"/dataset/{dataset_name}/{version_name}/download/json",
        headers=headers,
        json={
            **PARAMS,
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-60.590457916259766, -15.095079526355857],
                        [-60.60298919677734, -15.090936030923759],
                        [-60.60161590576172, -15.104774989795663],
                        [-60.590457916259766, -15.095079526355857],
                    ]
                ],
            },
        },
    )

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_download_raster_csv_with_geometry(
    generic_raster_version, apikey, async_client: AsyncClient, monkeypatch: MonkeyPatch
):
    dataset_name, version_name, _ = generic_raster_version
    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]
    headers = {"origin": origin, "x-api-key": api_key}

    async def httpx_response_coroutine(*args, **kwargs) -> httpx.Response:
        return httpx.Response(status_code=200, json={"status": "success", "data": None})

    monkeypatch.setattr(queries, "invoke_lambda", httpx_response_coroutine)

    response = await async_client.post(
        f"/dataset/{dataset_name}/{version_name}/download/csv",
        headers=headers,
        json={
            **PARAMS,
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-60.590457916259766, -15.095079526355857],
                        [-60.60298919677734, -15.090936030923759],
                        [-60.60161590576172, -15.104774989795663],
                        [-60.590457916259766, -15.095079526355857],
                    ]
                ],
            },
        },
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_download_by_aoi_raster_csv(
    generic_raster_version, apikey, async_client: AsyncClient
):
    dataset_name, version_name, _ = generic_raster_version
    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]
    headers = {"origin": origin, "x-api-key": api_key}

    with (
        patch(
            "app.models.pydantic.datamart.AdminAreaOfInterest.get_geostore_id",
            return_value=TEST_GEOSTORE_ID,
        ),
        patch(
            "app.routes.datasets.downloads.get_geostore", return_value=TEST_GEOSTORE
        ) as mock_get_geostore,
        patch(
            "app.routes.datasets.downloads._query_dataset_csv",
            return_value=StringIO("x,y\n,1,2"),
        ) as mock_query_dataset_csv,
    ):
        response = await async_client.get(
            f"/dataset/{dataset_name}/{version_name}/download_by_aoi/csv?sql={TEST_SQL}&aoi[type]=admin&aoi[country]=IDN&aoi[region]=9&aoi[subregion]=9&aoi[provider]=gadm&aoi[version]=4.1",
            headers=headers,
        )

        mock_get_geostore.assert_awaited_once_with(TEST_GEOSTORE_ID, GeostoreOrigin.rw)
        mock_query_dataset_csv.assert_awaited_once_with(
            dataset_name, version_name, TEST_SQL, TEST_GEOSTORE, ","
        )

        assert response.status_code == 200
        assert response.content == b"x,y\n,1,2"


@pytest.mark.asyncio
async def test_download_by_aoi_raster_json(
    generic_raster_version, apikey, async_client: AsyncClient
):
    dataset_name, version_name, _ = generic_raster_version
    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]
    headers = {"origin": origin, "x-api-key": api_key}

    with (
        patch(
            "app.models.pydantic.datamart.AdminAreaOfInterest.get_geostore_id",
            return_value=TEST_GEOSTORE_ID,
        ),
        patch(
            "app.routes.datasets.downloads.get_geostore", return_value=TEST_GEOSTORE
        ) as mock_get_geostore,
        patch(
            "app.routes.datasets.downloads._query_dataset_json",
            return_value={"data": [{"x": 1, "y": 2}]},
        ) as mock_query_dataset_json,
    ):
        response = await async_client.get(
            f"/dataset/{dataset_name}/{version_name}/download_by_aoi/json?sql={TEST_SQL}&aoi[type]=admin&aoi[country]=IDN&aoi[region]=9&aoi[subregion]=9&aoi[provider]=gadm&aoi[version]=4.1",
            headers=headers,
        )

        mock_get_geostore.assert_awaited_once_with(TEST_GEOSTORE_ID, GeostoreOrigin.rw)
        mock_query_dataset_json.assert_awaited_once_with(
            dataset_name, version_name, TEST_SQL, TEST_GEOSTORE
        )

        assert response.status_code == 200
        assert response.json() == {"data": [{"x": 1, "y": 2}]}
