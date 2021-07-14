import pytest
from httpx import AsyncClient

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
