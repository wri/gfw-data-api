import pytest
from _pytest.monkeypatch import MonkeyPatch
from httpx import AsyncClient, Response
from unittest.mock import AsyncMock, Mock

from app.models.enum.pixetl import Grid
from app.routes.datasets import queries
from app.utils import geostore
from tests_v2.utils import invoke_lambda_mocked
from tests_v2.fixtures.creation_options.versions import RASTER_CREATION_OPTIONS
from tests_v2.fixtures.sample_rw_geostore_response import geostore_common
from tests_v2.fixtures.otf_payload.otf_payload import sql, environment
from tests_v2.utils import custom_raster_version


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
    params = {"x-api-key": api_key, "sum": "area__ha"}

    monkeypatch.setattr(queries, "invoke_lambda", invoke_lambda_mocked)

    response = await async_client.get(
        f"/analysis/zonal/{geostore}", headers=headers, params=params
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
    params = {"x-api-key": api_key, "sum": "area__ha"}
    response = await async_client.get(
        f"/analysis/zonal/{geostore_huge}", headers=headers, params=params
    )

    assert response.status_code == 400


@pytest.mark.asyncio
async def test_raster_analysis_payload_shape(
    generic_dataset, async_client: AsyncClient, monkeypatch: MonkeyPatch
):
    dataset_name, _ = generic_dataset
    pixel_meaning: str = "date_conf"
    no_data_value = 0

    async with custom_raster_version(
        async_client,
        dataset_name,
        monkeypatch,
        pixel_meaning=pixel_meaning,
        no_data=no_data_value,
    ) as version_name:

        mock_invoke_lambda = AsyncMock(
            return_value=Response(200, json={"status": "success", "data": []})
        )
        monkeypatch.setattr(queries, "invoke_lambda", mock_invoke_lambda)

        mock_rw_get_geostore = Mock(
            geostore.rw_api.get_geostore, return_value=geostore_common
        )
        monkeypatch.setattr(geostore.rw_api, "get_geostore", mock_rw_get_geostore)

        _ = await async_client.get(
            f"/analysis/zonal/17076d5ea9f214a5bdb68cc40433addb?geostore_origin=rw&group_by=umd_tree_cover_loss__year&filters=is__umd_regional_primary_forest_2001&filters=umd_tree_cover_density_2000__30&sum=area__ha&start_date=2001"
        )
        payload = mock_invoke_lambda.call_args.args[1]
        assert payload["query"] == sql
        assert payload["environment"] == environment
        assert payload["geometry"] == geostore_common.geojson
