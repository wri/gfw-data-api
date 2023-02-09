from typing import Tuple
from unittest.mock import Mock

import pytest
from _pytest.monkeypatch import MonkeyPatch
from httpx import AsyncClient

from app.models.enum.pixetl import Grid
from app.models.pydantic.raster_analysis import DerivedLayer, SourceLayer
from app.routes.datasets import queries, versions
from app.routes.datasets.queries import _get_data_environment
from app.tasks import batch, delete_assets
from app.tasks.raster_tile_set_assets import raster_tile_set_assets
from tests_v2.fixtures.creation_options.versions import RASTER_CREATION_OPTIONS
from tests_v2.utils import (
    BatchJobMock,
    dict_function_closure,
    get_extent_mocked,
    int_function_closure,
    invoke_lambda_mocked,
    void_coroutine,
)


@pytest.mark.skip("Temporarily skip until we require API keys")
@pytest.mark.asyncio
async def test_query_dataset_without_api_key(
    generic_vector_source_version, async_client: AsyncClient
):
    dataset_name, version_name, _ = generic_vector_source_version
    params = {"sql": "select * from data"}

    response = await async_client.get(
        f"/dataset/{dataset_name}/{version_name}/query", params=params
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
    params = {"sql": "select count(*) as count from data"}

    response = await async_client.get(
        f"/dataset/{dataset_name}/{version_name}/query",
        params=params,
        headers=headers,
        follow_redirects=True,
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

    params = {"sql": "select count(*) as count from data"}

    response = await async_client.get(
        f"/dataset/{dataset_name}/{version_name}/query",
        params=params,
        headers=headers,
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert response.json()["data"][0]["count"] == 1


@pytest.mark.asyncio
async def test_query_dataset_raster_get(
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
    params = {"sql": "select count(*) from data", "geostore_id": geostore}

    response = await async_client.get(
        f"/dataset/{dataset_name}/{version_name}/query",
        params=params,
        headers=headers,
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert response.json()["status"] == "success"


@pytest.mark.asyncio
async def test_query_dataset_raster_post(
    generic_raster_version,
    apikey,
    geojson,
    monkeypatch: MonkeyPatch,
    async_client: AsyncClient,
):
    dataset_name, version_name, _ = generic_raster_version
    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]

    headers = {"origin": origin, "x-api-key": api_key}

    monkeypatch.setattr(queries, "invoke_lambda", invoke_lambda_mocked)
    payload = {
        "sql": "select count(*) from data",
        "geometry": geojson["features"][0]["geometry"],
    }

    response = await async_client.post(
        f"/dataset/{dataset_name}/{version_name}/query",
        json=payload,
        headers=headers,
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert response.json()["status"] == "success"


@pytest.mark.asyncio
async def test_redirect_post_query(
    generic_raster_version,
    apikey,
    geojson,
    monkeypatch: MonkeyPatch,
    async_client: AsyncClient,
):
    dataset_name, version_name, _ = generic_raster_version
    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]

    headers = {"origin": origin, "x-api-key": api_key}

    monkeypatch.setattr(queries, "invoke_lambda", invoke_lambda_mocked)
    payload = {
        "sql": "select count(*) from data",
        "geometry": geojson["features"][0]["geometry"],
    }

    response = await async_client.post(
        f"/dataset/{dataset_name}/{version_name}/query",
        headers=headers,
        json=payload,
        follow_redirects=False,
    )

    assert response.status_code == 308
    assert (
        response.headers["location"]
        == f"/dataset/{dataset_name}/{version_name}/query/json"
    )


@pytest.mark.asyncio
async def test_redirect_get_query(
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
    params = {"sql": "select count(*) from data", "geostore_id": geostore}

    response = await async_client.get(
        f"/dataset/{dataset_name}/{version_name}/query",
        headers=headers,
        params=params,
        follow_redirects=False,
    )

    assert response.status_code == 308
    assert (
        response.headers["location"]
        == f"/dataset/{dataset_name}/{version_name}/query/json?{response.request.url.query.decode('utf-8')}"
    )


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
    params = {"sql": "select count(*) from data", "geostore_id": geostore_huge}
    response = await async_client.get(
        f"/dataset/{dataset_name}/{version_name}/query",
        params=params,
        headers=headers,
        follow_redirects=True,
    )

    assert response.status_code == 400


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_1(
    generic_vector_source_version, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select current_catalog from mytable;",
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert response.json()["message"] == "Use of sql value functions is not allowed."


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_2(
    generic_vector_source_version, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select version() from mytable;",
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_3(
    generic_vector_source_version, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select has_any_column_privilege() from mytable;",
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_4(
    generic_vector_source_version, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select format_type() from mytable;",
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_5(
    generic_vector_source_version, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select col_description() from mytable;",
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_6(
    generic_vector_source_version, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select txid_current() from mytable;",
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_7(
    generic_vector_source_version, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select current_setting() from mytable;",
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_8(
    generic_vector_source_version, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select pg_cancel_backend() from mytable;",
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_9(
    generic_vector_source_version, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select brin_summarize_new_values() from mytable;",
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_10(
    generic_vector_source_version, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select doesnotexist() from mytable;",
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert response.json()["message"] == (
        "Bad request. function doesnotexist() does not exist\n"
        "HINT:  No function matches the given name and argument types. "
        "You might need to add explicit type casts."
    )


@pytest.mark.asyncio
async def test__get_data_environment_helpers_called_dateconf(
    async_client: AsyncClient,
    generic_dataset: Tuple[str, str],
    monkeypatch: MonkeyPatch,
):
    dataset_name, _ = generic_dataset
    version_name: str = "v1"

    # Patch all functions which reach out to external services
    # Basically we're leaving out everything but the DB entries being created
    batch_job_mock = BatchJobMock()
    monkeypatch.setattr(versions, "_verify_source_file_access", void_coroutine)
    monkeypatch.setattr(batch, "submit_batch_job", batch_job_mock.submit_batch_job)
    monkeypatch.setattr(delete_assets, "delete_s3_objects", int_function_closure(1))
    monkeypatch.setattr(raster_tile_set_assets, "get_extent", get_extent_mocked)
    monkeypatch.setattr(
        delete_assets, "flush_cloudfront_cache", dict_function_closure({})
    )

    pixel_meaning: str = "date_conf"
    no_data_value = 0

    # Create version
    await async_client.put(
        f"/dataset/{dataset_name}/{version_name}",
        json={
            "creation_options": {
                **RASTER_CREATION_OPTIONS,
                **{"pixel_meaning": pixel_meaning, "no_data": no_data_value},
            }
        },
    )

    await async_client.patch(
        f"/dataset/{dataset_name}/{version_name}", json={"is_latest": True}
    )

    grid: Grid = Grid(RASTER_CREATION_OPTIONS["grid"])

    _get_source_layer_mock = Mock(
        queries._get_source_layer,
        return_value=SourceLayer(
            name="foo", source_uri="some_source_uri", grid=grid, no_data=no_data_value
        ),
    )
    monkeypatch.setattr(queries, "_get_source_layer", _get_source_layer_mock)
    _get_date_conf_derived_layers_mock = Mock(
        queries._get_date_conf_derived_layers, return_value=[]
    )
    monkeypatch.setattr(
        queries, "_get_date_conf_derived_layers", _get_date_conf_derived_layers_mock
    )

    _ = await _get_data_environment(grid)

    assert _get_date_conf_derived_layers_mock.call_args.args == (
        f"{dataset_name}__{pixel_meaning}",
        no_data_value,
    )

    assert _get_source_layer_mock.call_args.args == (
        f"s3://gfw-data-lake-test/{dataset_name}/v1/raster/epsg-4326/{grid.value}/{pixel_meaning}/geotiff/"
        + "{tile_id}.tif",
        f"{dataset_name}__{pixel_meaning}",
        grid,
        no_data_value,
        None,
    )


@pytest.mark.asyncio
async def test__get_data_environment_helpers_called_area_density(
    async_client: AsyncClient,
    generic_dataset: Tuple[str, str],
    monkeypatch: MonkeyPatch,
):
    dataset_name, _ = generic_dataset
    version_name: str = "v1"

    # Patch all functions which reach out to external services
    # Basically we're leaving out everything but the DB entries being created
    batch_job_mock = BatchJobMock()
    monkeypatch.setattr(versions, "_verify_source_file_access", void_coroutine)
    monkeypatch.setattr(batch, "submit_batch_job", batch_job_mock.submit_batch_job)
    monkeypatch.setattr(delete_assets, "delete_s3_objects", int_function_closure(1))
    monkeypatch.setattr(raster_tile_set_assets, "get_extent", get_extent_mocked)
    monkeypatch.setattr(
        delete_assets, "flush_cloudfront_cache", dict_function_closure({})
    )

    pixel_meaning: str = "hamsters_ha-1"
    no_data_value = None

    # Create version
    await async_client.put(
        f"/dataset/{dataset_name}/{version_name}",
        json={
            "creation_options": {
                **RASTER_CREATION_OPTIONS,
                **{"pixel_meaning": pixel_meaning, "no_data": no_data_value},
            }
        },
    )

    await async_client.patch(
        f"/dataset/{dataset_name}/{version_name}", json={"is_latest": True}
    )

    grid: Grid = Grid(RASTER_CREATION_OPTIONS["grid"])

    _get_source_layer_mock = Mock(
        queries._get_source_layer,
        return_value=SourceLayer(
            name="foo", source_uri="some_source_uri", grid=grid, no_data=no_data_value
        ),
    )
    monkeypatch.setattr(queries, "_get_source_layer", _get_source_layer_mock)

    _get_area_density_layer_mock = Mock(
        queries._get_area_density_layer, return_value=Mock(DerivedLayer, autospec=True)
    )
    monkeypatch.setattr(
        queries, "_get_area_density_layer", _get_area_density_layer_mock
    )

    _ = await _get_data_environment(grid)

    assert _get_area_density_layer_mock.call_args.args == (
        f"{dataset_name}__{pixel_meaning}",
        no_data_value,
    )

    assert _get_source_layer_mock.call_args.args == (
        f"s3://gfw-data-lake-test/{dataset_name}/v1/raster/epsg-4326/{grid.value}/{pixel_meaning}/geotiff/"
        + "{tile_id}.tif",
        f"{dataset_name}__{pixel_meaning}",
        grid,
        no_data_value,
        None,
    )


@pytest.mark.asyncio
async def test__get_data_environment_helper_called(
    async_client: AsyncClient,
    generic_dataset: Tuple[str, str],
    monkeypatch: MonkeyPatch,
):
    dataset_name, _ = generic_dataset
    version_name: str = "v1"

    # Patch all functions which reach out to external services
    # Basically we're leaving out everything but the DB entries being created
    batch_job_mock = BatchJobMock()
    monkeypatch.setattr(versions, "_verify_source_file_access", void_coroutine)
    monkeypatch.setattr(batch, "submit_batch_job", batch_job_mock.submit_batch_job)
    monkeypatch.setattr(delete_assets, "delete_s3_objects", int_function_closure(1))
    monkeypatch.setattr(raster_tile_set_assets, "get_extent", get_extent_mocked)
    monkeypatch.setattr(
        delete_assets, "flush_cloudfront_cache", dict_function_closure({})
    )

    pixel_meaning: str = "foo"
    no_data_value = 255

    # Create version
    await async_client.put(
        f"/dataset/{dataset_name}/{version_name}",
        json={
            "creation_options": {
                **RASTER_CREATION_OPTIONS,
                **{"pixel_meaning": pixel_meaning, "no_data": no_data_value},
            }
        },
    )

    await async_client.patch(
        f"/dataset/{dataset_name}/{version_name}", json={"is_latest": True}
    )

    grid: Grid = Grid(RASTER_CREATION_OPTIONS["grid"])

    _get_source_layer_mock = Mock(
        queries._get_source_layer,
        return_value=SourceLayer(
            name="foo", source_uri="some_source_uri", grid=grid, no_data=no_data_value
        ),
    )
    monkeypatch.setattr(queries, "_get_source_layer", _get_source_layer_mock)

    _ = await _get_data_environment(grid)

    assert _get_source_layer_mock.call_args.args == (
        f"s3://gfw-data-lake-test/{dataset_name}/v1/raster/epsg-4326/{grid.value}/{pixel_meaning}/geotiff/"
        + "{tile_id}.tif",
        f"{dataset_name}__{pixel_meaning}",
        grid,
        no_data_value,
        None,
    )
