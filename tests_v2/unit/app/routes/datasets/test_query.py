from typing import Tuple
from unittest.mock import Mock
from urllib.parse import parse_qsl, urlparse
from uuid import UUID

import pytest
from _pytest.monkeypatch import MonkeyPatch
from httpx import AsyncClient

from app.models.enum.pixetl import Grid
from app.models.pydantic.raster_analysis import DerivedLayer, SourceLayer
from app.routes.datasets import queries
from app.routes.datasets.queries import _get_data_environment
from tests_v2.fixtures.creation_options.versions import RASTER_CREATION_OPTIONS
from tests_v2.utils import (
    custom_raster_version,
    invoke_lambda_mocked,
    start_batch_execution_mocked,
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
async def test_fields_dataset_raster(generic_raster_version, async_client: AsyncClient):
    dataset_name, version_name, _ = generic_raster_version
    response = await async_client.get(f"/dataset/{dataset_name}/{version_name}/fields")

    assert response.status_code == 200
    data = response.json()["data"]
    assert len(data) == 4
    assert data[0]["pixel_meaning"] == "area__ha"
    assert data[0]["values_table"] is None
    assert data[1]["pixel_meaning"] == "latitude"
    assert data[1]["values_table"] is None
    assert data[2]["pixel_meaning"] == "longitude"
    assert data[2]["values_table"] is None
    assert data[3]["pixel_meaning"] == "my_first_dataset__year"
    assert data[3]["values_table"] is None


@pytest.mark.asyncio
async def test_query_dataset_raster_bad_get(
    generic_raster_version,
    apikey,
    geostore_bad,
    monkeypatch: MonkeyPatch,
    async_client: AsyncClient,
):
    dataset_name, version_name, _ = generic_raster_version
    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]

    headers = {"origin": origin, "x-api-key": api_key}

    monkeypatch.setattr(queries, "invoke_lambda", invoke_lambda_mocked)
    params = {"sql": "select count(*) from data", "geostore_id": geostore_bad}

    response = await async_client.get(
        f"/dataset/{dataset_name}/{version_name}/query",
        params=params,
        headers=headers,
        follow_redirects=True,
    )

    assert response.status_code == 400


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
    assert parse_qsl(
        urlparse(response.headers["location"]).query, strict_parsing=True
    ) == parse_qsl(
        urlparse(
            f"/dataset/{dataset_name}/{version_name}/query/json?{response.request.url.query.decode('utf-8')}"
        ).query,
        strict_parsing=True,
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
    generic_vector_source_version, apikey, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]
    headers = {"origin": origin, "x-api-key": api_key}

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select current_catalog from mytable;",
        headers=headers,
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert response.json()["message"] == "Use of sql value functions is not allowed."


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_2(
    generic_vector_source_version, apikey, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]
    headers = {"origin": origin, "x-api-key": api_key}

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select version() from mytable;",
        headers=headers,
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_3(
    generic_vector_source_version, apikey, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]
    headers = {"origin": origin, "x-api-key": api_key}

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select has_any_column_privilege() from mytable;",
        headers=headers,
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_4(
    generic_vector_source_version, apikey, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]
    headers = {"origin": origin, "x-api-key": api_key}

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select format_type() from mytable;",
        headers=headers,
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_5(
    generic_vector_source_version, apikey, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]
    headers = {"origin": origin, "x-api-key": api_key}

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select col_description() from mytable;",
        headers=headers,
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_6(
    generic_vector_source_version, apikey, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]
    headers = {"origin": origin, "x-api-key": api_key}

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select txid_current() from mytable;",
        headers=headers,
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_7(
    generic_vector_source_version, apikey, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]
    headers = {"origin": origin, "x-api-key": api_key}

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select current_setting() from mytable;",
        headers=headers,
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_8(
    generic_vector_source_version, apikey, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]
    headers = {"origin": origin, "x-api-key": api_key}

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select pg_cancel_backend() from mytable;",
        headers=headers,
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_9(
    generic_vector_source_version, apikey, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]
    headers = {"origin": origin, "x-api-key": api_key}

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select brin_summarize_new_values() from mytable;",
        headers=headers,
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "Use of admin, system or private functions is not allowed."
    )


@pytest.mark.asyncio()
async def test_query_vector_asset_disallowed_10(
    generic_vector_source_version, apikey, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version

    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]
    headers = {"origin": origin, "x-api-key": api_key}

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select doesnotexist() from mytable;",
        headers=headers,
        follow_redirects=True,
    )
    assert response.status_code == 400
    assert response.json()["message"] == (
        "Bad request. function doesnotexist() does not exist\n"
        "HINT:  No function matches the given name and argument types. "
        "You might need to add explicit type casts."
    )


@pytest.mark.asyncio()
async def test_query_licensed_disallowed_11(
    licensed_version, apikey, async_client: AsyncClient
):
    dataset, version, _ = licensed_version

    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]
    headers = {"origin": origin, "x-api-key": api_key}

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/query?sql=select(*) from mytable;",
        headers=headers,
        follow_redirects=True,
    )
    assert response.status_code == 401
    assert response.json()["message"] == ("Unauthorized query on a restricted dataset")


@pytest.mark.asyncio
@pytest.mark.skip("Temporarily skip while _get_data_environment is being cached")
async def test__get_data_environment_helpers_called_dateconf(
    async_client: AsyncClient,
    generic_dataset: Tuple[str, str],
    monkeypatch: MonkeyPatch,
):
    dataset_name, _ = generic_dataset
    pixel_meaning: str = "date_conf"
    no_data_value = 0
    grid: Grid = Grid(RASTER_CREATION_OPTIONS["grid"])

    async with custom_raster_version(
        async_client,
        dataset_name,
        monkeypatch,
        pixel_meaning=pixel_meaning,
        no_data=no_data_value,
    ) as version_name:

        _get_source_layer_mock = Mock(
            queries._get_source_layer,
            return_value=SourceLayer(
                name="some_layer_name",
                source_uri="some_source_uri",
                grid=grid,
                no_data=no_data_value,
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
            f"s3://gfw-data-lake-test/{dataset_name}/{version_name}/raster/epsg-4326/{grid.value}/{pixel_meaning}/geotiff/"
            + "{tile_id}.tif",
            f"{dataset_name}__{pixel_meaning}",
            grid,
            no_data_value,
            None,
        )


@pytest.mark.asyncio
@pytest.mark.skip("Temporarily skip while _get_data_environment is being cached")
async def test__get_data_environment_helpers_called_area_density(
    async_client: AsyncClient,
    generic_dataset: Tuple[str, str],
    monkeypatch: MonkeyPatch,
):
    dataset_name, _ = generic_dataset
    pixel_meaning: str = "hamsters_ha-1"
    no_data_value = None
    grid: Grid = Grid(RASTER_CREATION_OPTIONS["grid"])

    async with custom_raster_version(
        async_client,
        dataset_name,
        monkeypatch,
        pixel_meaning=pixel_meaning,
        no_data=no_data_value,
    ) as version_name:

        _get_source_layer_mock = Mock(
            queries._get_source_layer,
            return_value=SourceLayer(
                name="some_layer_name",
                source_uri="some_source_uri",
                grid=grid,
                no_data=no_data_value,
            ),
        )
        monkeypatch.setattr(queries, "_get_source_layer", _get_source_layer_mock)

        _get_area_density_layer_mock = Mock(
            queries._get_area_density_layer,
            return_value=Mock(DerivedLayer, autospec=True),
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
            f"s3://gfw-data-lake-test/{dataset_name}/{version_name}/raster/epsg-4326/{grid.value}/{pixel_meaning}/geotiff/"
            + "{tile_id}.tif",
            f"{dataset_name}__{pixel_meaning}",
            grid,
            no_data_value,
            None,
        )


@pytest.mark.asyncio
@pytest.mark.skip("Temporarily skip while _get_data_environment is being cached")
async def test__get_data_environment_helper_called(
    async_client: AsyncClient,
    generic_dataset: Tuple[str, str],
    monkeypatch: MonkeyPatch,
):
    dataset_name, _ = generic_dataset

    pixel_meaning: str = "foo"
    no_data_value = 255
    grid: Grid = Grid(RASTER_CREATION_OPTIONS["grid"])

    async with custom_raster_version(
        async_client,
        dataset_name,
        monkeypatch,
        pixel_meaning=pixel_meaning,
        no_data=no_data_value,
    ) as version_name:

        _get_source_layer_mock = Mock(
            queries._get_source_layer,
            return_value=SourceLayer(
                name="some_layer_name",
                source_uri="some_source_uri",
                grid=grid,
                no_data=no_data_value,
            ),
        )
        monkeypatch.setattr(queries, "_get_source_layer", _get_source_layer_mock)

        _ = await _get_data_environment(grid)

        assert _get_source_layer_mock.call_args.args == (
            f"s3://gfw-data-lake-test/{dataset_name}/{version_name}/raster/epsg-4326/{grid.value}/{pixel_meaning}/geotiff/"
            + "{tile_id}.tif",
            f"{dataset_name}__{pixel_meaning}",
            grid,
            no_data_value,
            None,
        )


@pytest.mark.asyncio
async def test_query_batch_feature_collection(
    generic_raster_version,
    apikey,
    monkeypatch: MonkeyPatch,
    async_client: AsyncClient,
):
    dataset_name, version_name, _ = generic_raster_version
    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]

    headers = {"origin": origin, "x-api-key": api_key}

    monkeypatch.setattr(queries, "_start_batch_execution", start_batch_execution_mocked)
    payload = {
        "sql": "select count(*) from data",
        "feature_collection": FEATURE_COLLECTION,
        "id_field": "id",
    }

    response = await async_client.post(
        f"/dataset/{dataset_name}/{version_name}/query/batch",
        json=payload,
        headers=headers,
    )

    print(response.json())
    assert response.status_code == 202

    data = response.json()["data"]

    # assert valid UUID
    try:
        uuid = UUID(data["job_id"])
    except ValueError:
        assert False

    assert str(uuid) == data["job_id"]
    assert data["job_link"].endswith(f"/job/{data['job_id']}")

    assert data["status"] == "pending"

    assert response.json()["status"] == "success"


@pytest.mark.asyncio
async def test_query_batch_uri(
    generic_raster_version,
    apikey,
    monkeypatch: MonkeyPatch,
    async_client: AsyncClient,
):
    dataset_name, version_name, _ = generic_raster_version
    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]

    headers = {"origin": origin, "x-api-key": api_key}

    monkeypatch.setattr(queries, "_start_batch_execution", start_batch_execution_mocked)
    monkeypatch.setattr(queries, "_verify_source_file_access", lambda source_uris: True)

    payload = {
        "sql": "select count(*) from data",
        "uri": "s3://path/to/files",
        "id_field": "id",
    }

    response = await async_client.post(
        f"/dataset/{dataset_name}/{version_name}/query/batch",
        json=payload,
        headers=headers,
    )

    print(response.json())
    assert response.status_code == 202

    data = response.json()["data"]

    # assert valid UUID
    try:
        uuid = UUID(data["job_id"])
    except ValueError:
        assert False

    assert str(uuid) == data["job_id"]

    assert data["status"] == "pending"

    assert response.json()["status"] == "success"


FEATURE_COLLECTION = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {
                "id": 1,
            },
            "geometry": {
                "coordinates": [
                    [
                        [-57.43488539218248, -11.378524299779286],
                        [-57.43488539218248, -11.871111619666053],
                        [-56.950732779425806, -11.871111619666053],
                        [-56.950732779425806, -11.378524299779286],
                        [-57.43488539218248, -11.378524299779286],
                    ]
                ],
                "type": "Polygon",
            },
        },
        {
            "type": "Feature",
            "properties": {
                "id": 2,
            },
            "geometry": {
                "coordinates": [
                    [
                        [-55.84751191303597, -11.845408946893727],
                        [-55.84751191303597, -12.293066281588139],
                        [-55.32975635387763, -12.293066281588139],
                        [-55.32975635387763, -11.845408946893727],
                        [-55.84751191303597, -11.845408946893727],
                    ]
                ],
                "type": "Polygon",
            },
        },
        {
            "type": "Feature",
            "properties": {
                "id": 3,
            },
            "geometry": {
                "coordinates": [
                    [
                        [-58.36172075077614, -12.835185539172727],
                        [-58.36172075077614, -13.153322454532116],
                        [-57.98648069126074, -13.153322454532116],
                        [-57.98648069126074, -12.835185539172727],
                        [-58.36172075077614, -12.835185539172727],
                    ]
                ],
                "type": "Polygon",
            },
        },
    ],
}
