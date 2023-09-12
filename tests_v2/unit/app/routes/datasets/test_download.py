import httpx
import pytest
from _pytest.monkeypatch import MonkeyPatch
from httpx import AsyncClient

from app.routes.datasets import queries

PARAMS = {"sql": "select count(*) as count from data"}
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
