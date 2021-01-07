from unittest.mock import patch
from uuid import UUID

import boto3
import httpx
import pytest
from botocore.exceptions import ClientError

from app.application import ContextEngine
from app.crud import tasks
from app.crud.assets import update_asset
from app.models.enum.symbology import ColorMapType
from app.settings.globals import AWS_REGION, DATA_LAKE_BUCKET, TILE_CACHE_BUCKET
from app.utils.aws import get_s3_client
from tests.tasks import MockCloudfrontClient
from tests.utils import check_tasks_status, create_default_asset, poll_jobs


@pytest.mark.asyncio
async def test_assets(async_client):
    """Basic tests of asset endpoint behavior."""
    # Add a dataset, version, and default asset
    dataset = "test"
    version = "v20200626"

    asset = await create_default_asset(
        dataset, version, async_client=async_client, execute_batch_jobs=False
    )
    asset_id = asset["asset_id"]

    # Verify that the asset and version are in state "pending"
    version_resp = await async_client.get(f"/dataset/{dataset}/{version}")
    assert version_resp.json()["data"]["status"] == "pending"

    asset_resp = await async_client.get(f"/asset/{asset_id}")
    assert asset_resp.json()["data"]["status"] == "pending"

    # Try adding a non-default asset, which shouldn't work while the version
    # is still in "pending" status
    asset_payload = {
        "asset_type": "Database table",
        "is_managed": False,
        "creation_options": {"delimiter": ","},
    }
    create_asset_resp = await async_client.post(
        f"/dataset/{dataset}/{version}/assets", json=asset_payload
    )
    assert create_asset_resp.json()["message"] == (
        "Version status is currently `pending`. "
        "Please retry once version is in status `saved`"
    )
    assert create_asset_resp.json()["status"] == "failed"

    # Now add a task changelog of status "failed" which should make the
    # version status "failed". Try to add a non-default asset again, which
    # should fail as well but with a different explanation.
    get_resp = await async_client.get(f"/asset/{asset_id}/tasks")
    task_list = get_resp.json()["data"]
    sample_task_id = task_list[0]["task_id"]
    patch_payload = {
        "change_log": [
            {
                "date_time": "2020-06-25 14:30:00",
                "status": "failed",
                "message": "Bad Luck!",
                "detail": "None",
            }
        ]
    }
    patch_resp = await async_client.patch(f"/task/{sample_task_id}", json=patch_payload)
    assert patch_resp.json()["status"] == "success"

    create_asset_resp = await async_client.post(
        f"/dataset/{dataset}/{version}/assets", json=asset_payload
    )
    assert create_asset_resp.json()["status"] == "failed"
    assert create_asset_resp.json()["message"] == (
        "Version status is `failed`. Cannot add any assets."
    )


@pytest.mark.asyncio
async def test_auxiliary_raster_asset(async_client, batch_client, httpd):
    """"""
    _, logs = batch_client

    # Add a dataset, version, and default asset
    dataset = "test_auxiliary_raster_asset"
    version = "v1.0.0"
    primary_grid = "90/27008"
    auxiliary_grid = "90/9984"

    s3_client = boto3.client(
        "s3", region_name=AWS_REGION, endpoint_url="http://motoserver:5000"
    )

    pixetl_output_files = [
        f"{dataset}/{version}/raster/epsg-4326/{auxiliary_grid}/gfw_fid/gdal-geotiff/extent.geojson",
        f"{dataset}/{version}/raster/epsg-4326/{auxiliary_grid}/gfw_fid/geotiff/extent.geojson",
        f"{dataset}/{version}/raster/epsg-4326/{auxiliary_grid}/gfw_fid/gdal-geotiff/tiles.geojson",
        f"{dataset}/{version}/raster/epsg-4326/{auxiliary_grid}/gfw_fid/geotiff/tiles.geojson",
        f"{dataset}/{version}/raster/epsg-4326/{auxiliary_grid}/gfw_fid/gdal-geotiff/90N_000E.tif",
        f"{dataset}/{version}/raster/epsg-4326/{auxiliary_grid}/gfw_fid/geotiff/90N_000E.tif",
    ]

    for key in pixetl_output_files:
        s3_client.delete_object(Bucket=DATA_LAKE_BUCKET, Key=key)

    raster_version_payload = {
        "creation_options": {
            "source_type": "raster",
            "source_uri": [f"s3://{DATA_LAKE_BUCKET}/test/v1.1.1/raw/tiles.geojson"],
            "source_driver": "GeoTIFF",
            "data_type": "uint16",
            "pixel_meaning": "percent",
            "grid": primary_grid,
            "resampling": "nearest",
            "overwrite": True,
            "subset": "90N_000E",
            "no_data": 0,
        },
        "metadata": {},
    }
    asset = await create_default_asset(
        dataset,
        version,
        version_payload=raster_version_payload,
        async_client=async_client,
        execute_batch_jobs=True,
    )
    asset_id = asset["asset_id"]

    # Verify that the asset and version are in state "saved"
    version_resp = await async_client.get(f"/dataset/{dataset}/{version}")
    assert version_resp.json()["data"]["status"] == "saved"

    asset_resp = await async_client.get(f"/asset/{asset_id}")
    assert asset_resp.json()["data"]["status"] == "saved"

    # Flush requests list so we're starting fresh
    httpx.delete(f"http://localhost:{httpd.server_port}")

    # Try adding a non-default raster tile asset based on the default
    asset_payload = {
        "asset_type": "Raster tile set",
        "is_managed": True,
        "creation_options": {
            "data_type": "uint16",
            "pixel_meaning": "gfw_fid",
            "grid": auxiliary_grid,
            "resampling": "nearest",
            "overwrite": True,
            "subset": "90N_000E",
            "no_data": 0,
        },
    }

    create_asset_resp = await async_client.post(
        f"/dataset/{dataset}/{version}/assets", json=asset_payload
    )
    resp_json = create_asset_resp.json()
    assert resp_json["status"] == "success"
    assert resp_json["data"]["status"] == "pending"
    asset_id = resp_json["data"]["asset_id"]

    # wait until batch jobs are done.
    tasks_rows = await tasks.get_tasks(asset_id)
    task_ids = [str(task.task_id) for task in tasks_rows]
    status = await poll_jobs(task_ids, logs=logs, async_client=async_client)
    assert status == "saved"

    asset_resp = await async_client.get(f"/asset/{asset_id}")
    assert asset_resp.json()["data"]["status"] == "saved"

    for key in pixetl_output_files:
        try:
            s3_client.head_object(Bucket=DATA_LAKE_BUCKET, Key=key)
        except ClientError:
            raise AssertionError(f"Key {key} doesn't exist!")


@pytest.mark.asyncio
async def test_auxiliary_vector_asset(async_client, batch_client, httpd):
    """"""
    _, logs = batch_client

    # Add a dataset, version, and default asset
    dataset = "test_vector"
    version = "v1.1.1"

    s3_client = boto3.client(
        "s3", region_name=AWS_REGION, endpoint_url="http://motoserver:5000"
    )

    pixetl_output_files = [
        f"{dataset}/{version}/raster/epsg-4326/90/27008/gfw_fid/gdal-geotiff/extent.geojson",
        f"{dataset}/{version}/raster/epsg-4326/90/27008/gfw_fid/geotiff/extent.geojson",
        f"{dataset}/{version}/raster/epsg-4326/90/27008/gfw_fid/gdal-geotiff/tiles.geojson",
        f"{dataset}/{version}/raster/epsg-4326/90/27008/gfw_fid/geotiff/tiles.geojson",
        f"{dataset}/{version}/raster/epsg-4326/90/27008/gfw_fid/gdal-geotiff/90N_000E.tif",
        f"{dataset}/{version}/raster/epsg-4326/90/27008/gfw_fid/geotiff/90N_000E.tif",
    ]

    for key in pixetl_output_files:
        s3_client.delete_object(Bucket=DATA_LAKE_BUCKET, Key=key)

    asset = await create_default_asset(
        dataset, version, async_client=async_client, execute_batch_jobs=True
    )
    asset_id = asset["asset_id"]

    # Verify that the asset and version are in state "saved"
    version_resp = await async_client.get(f"/dataset/{dataset}/{version}")
    assert version_resp.json()["data"]["status"] == "saved"

    asset_resp = await async_client.get(f"/asset/{asset_id}")
    assert asset_resp.json()["data"]["status"] == "saved"

    # Flush requests list so we're starting fresh
    httpx.delete(f"http://localhost:{httpd.server_port}")

    # Try adding a non-default raster tile set asset based on the default
    # vector asset
    asset_payload = {
        "asset_type": "Raster tile set",
        "asset_uri": "http://www.osnews.com",
        "is_managed": True,
        "creation_options": {
            "data_type": "uint16",
            "pixel_meaning": "gfw_fid",
            "grid": "90/27008",
            "resampling": "nearest",
            "overwrite": True,
            "subset": "90N_000E",
        },
    }

    create_asset_resp = await async_client.post(
        f"/dataset/{dataset}/{version}/assets", json=asset_payload
    )
    resp_json = create_asset_resp.json()
    assert resp_json["status"] == "success"
    assert resp_json["data"]["status"] == "pending"
    asset_id = resp_json["data"]["asset_id"]

    # wait until batch jobs are done.
    tasks_rows = await tasks.get_tasks(asset_id)
    task_ids = [str(task.task_id) for task in tasks_rows]
    status = await poll_jobs(task_ids, logs=logs, async_client=async_client)
    assert status == "saved"

    asset_resp = await async_client.get(f"/asset/{asset_id}")
    assert asset_resp.json()["data"]["status"] == "saved"

    for key in pixetl_output_files:
        try:
            s3_client.head_object(Bucket=DATA_LAKE_BUCKET, Key=key)
        except ClientError:
            raise AssertionError(f"Key {key} doesn't exist!")


@pytest.mark.asyncio
async def test_asset_bad_requests(async_client, batch_client, httpd):
    """"""
    _, logs = batch_client

    # Add a dataset, version, and default asset
    dataset = "test_bad_requests"
    version = "v1.1.1"

    _ = await create_default_asset(
        dataset, version, async_client=async_client, execute_batch_jobs=False
    )

    # Flush requests list so we're starting fresh
    httpx.delete(f"http://localhost:{httpd.server_port}")

    # Try adding a non-default raster tile set asset with an extra field "foo"
    asset_payload = {
        "asset_type": "Raster tile set",
        "is_managed": True,
        "foo": "foo",  # The extra field
        "creation_options": {
            "data_type": "uint16",
            "pixel_meaning": "gfw_fid",
            "grid": "90/27008",
            "resampling": "nearest",
            "overwrite": True,
            "subset": "90N_000E",
        },
    }
    create_asset_resp = await async_client.post(
        f"/dataset/{dataset}/{version}/assets", json=asset_payload
    )
    resp_json = create_asset_resp.json()
    assert resp_json["status"] == "failed"
    assert resp_json["message"] == [
        {
            "loc": ["body", "foo"],
            "msg": "extra fields not permitted",
            "type": "value_error.extra",
        }
    ]

    # Try adding a non-default raster tile set asset missing the
    # "creation_options" field (and toss the extra "foo" field from before)
    del asset_payload["foo"]
    del asset_payload["creation_options"]
    create_asset_resp = await async_client.post(
        f"/dataset/{dataset}/{version}/assets", json=asset_payload
    )
    resp_json = create_asset_resp.json()
    assert resp_json["status"] == "failed"
    assert resp_json["message"] == [
        {
            "loc": ["body", "creation_options"],
            "msg": "field required",
            "type": "value_error.missing",
        }
    ]


@pytest.mark.asyncio
async def test_raster_tile_cache_asset(async_client, batch_client, httpd):
    """"""
    _, logs = batch_client

    # Add a dataset, version, and default (raster tile set) asset
    dataset = "test_raster_tile_cache_asset"
    version = "v1.0.0"
    primary_grid = "90/27008"

    pixel_meaning = "date_conf"
    raster_version_payload = {
        "creation_options": {
            "source_type": "raster",
            "source_uri": [f"s3://{DATA_LAKE_BUCKET}/test/v1.1.1/raw/tiles.geojson"],
            "source_driver": "GeoTIFF",
            "data_type": "uint16",
            "no_data": 0,
            "pixel_meaning": pixel_meaning,
            "grid": primary_grid,
            "resampling": "nearest",
            "overwrite": True,
        },
        "metadata": {},
    }

    asset = await create_default_asset(
        dataset,
        version,
        version_payload=raster_version_payload,
        async_client=async_client,
        execute_batch_jobs=True,
    )
    default_asset_id = asset["asset_id"]

    await check_tasks_status(async_client, logs, [default_asset_id])

    # Verify that the asset and version are in state "saved"
    version_resp = await async_client.get(f"/dataset/{dataset}/{version}")
    assert version_resp.json()["data"]["status"] == "saved"

    asset_resp = await async_client.get(f"/asset/{default_asset_id}")
    assert asset_resp.json()["data"]["status"] == "saved"

    # test_files = [
    #     f"{pixetl_output_files_prefix}/{pixel_meaning}/{test_file}"
    #     for test_file in pixetl_test_files
    # ]
    # _check_s3_file_present(DATA_LAKE_BUCKET, test_files)

    ########################

    symbology_checks = [
        {
            "wm_tile_set_assets": ["date_conf", "intensity", "rgb_encoded"],
            "symbology": {"type": ColorMapType.date_conf_intensity},
        },
        {
            "wm_tile_set_assets": ["date_conf", f"date_conf_{ColorMapType.gradient}"],
            "symbology": {
                "type": ColorMapType.gradient,
                "colormap": {
                    1: {"red": 255, "green": 0, "blue": 0},
                    19: {"red": 0, "green": 0, "blue": 255},
                },
            },
        },
        {
            "wm_tile_set_assets": [f"date_conf_{ColorMapType.discrete}"],
            "symbology": {
                "type": ColorMapType.discrete,
                "colormap": {
                    1: {"red": 255, "green": 0, "blue": 0},
                    2: {"red": 255, "green": 0, "blue": 0},
                    3: {"red": 255, "green": 20, "blue": 0},
                    4: {"red": 255, "green": 40, "blue": 0},
                    5: {"red": 255, "green": 60, "blue": 0},
                    6: {"red": 255, "green": 80, "blue": 0},
                    7: {"red": 255, "green": 100, "blue": 0},
                    8: {"red": 255, "green": 120, "blue": 0},
                    9: {"red": 255, "green": 140, "blue": 0},
                    10: {"red": 255, "green": 160, "blue": 0},
                    11: {"red": 255, "green": 180, "blue": 0},
                    12: {"red": 255, "green": 200, "blue": 0},
                    13: {"red": 255, "green": 220, "blue": 0},
                    14: {"red": 255, "green": 240, "blue": 0},
                    15: {"red": 255, "green": 255, "blue": 0},
                    16: {"red": 255, "green": 255, "blue": 20},
                    17: {"red": 255, "green": 255, "blue": 40},
                    18: {"red": 255, "green": 255, "blue": 60},
                    19: {"red": 255, "green": 255, "blue": 80},
                },
            },
        },
    ]

    for check in symbology_checks:
        # Flush requests list so we're starting fresh
        httpx.delete(f"http://localhost:{httpd.server_port}")

        await _test_raster_tile_cache(
            dataset, version, default_asset_id, async_client, logs, **check,
        )


async def _test_raster_tile_cache(
    dataset: str,
    version: str,
    default_asset_id: UUID,
    async_client,
    logs,
    wm_tile_set_assets,
    symbology,
):
    pixetl_output_files_prefix = f"{dataset}/{version}/raster/epsg-3857/zoom_1"
    pixetl_test_files = [
        "geotiff/extent.geojson",
        "geotiff/tiles.geojson",
        "geotiff/000R_000C.tif",
    ]

    _delete_s3_files(DATA_LAKE_BUCKET, pixetl_output_files_prefix)
    _delete_s3_files(TILE_CACHE_BUCKET, f"{dataset}/{version}")

    print("FINISHED CLEANING UP")

    # Add a tile cache asset based on the raster tile set
    asset_payload = {
        "asset_type": "Raster tile cache",
        "is_managed": True,
        "creation_options": {
            "source_asset_id": default_asset_id,
            "min_zoom": 0,
            "max_zoom": 2,
            "max_static_zoom": 1,
            "symbology": symbology,
            "implementation": symbology["type"],
        },
        "metadata": {},
    }

    old_assets_resp = await async_client.get(f"/dataset/{dataset}/{version}/assets")
    old_asset_ids = set([asset["asset_id"] for asset in old_assets_resp.json()["data"]])

    create_asset_resp = await async_client.post(
        f"/dataset/{dataset}/{version}/assets", json=asset_payload
    )
    resp_json = create_asset_resp.json()
    print(resp_json)
    assert resp_json["status"] == "success"
    assert resp_json["data"]["status"] == "pending"

    tile_cache_asset_id = resp_json["data"]["asset_id"]

    new_assets_resp = await async_client.get(f"/dataset/{dataset}/{version}/assets")
    new_asset_ids = (
        set([asset["asset_id"] for asset in new_assets_resp.json()["data"]])
        - old_asset_ids
    )

    await check_tasks_status(async_client, logs, new_asset_ids)

    # Make sure the raster tile cache asset is in "saved" state
    asset_resp = await async_client.get(f"/asset/{tile_cache_asset_id}")
    assert asset_resp.json()["data"]["status"] == "saved"

    # Check if file for all expected assets are present
    for pixel_meaning in wm_tile_set_assets:
        test_files = [
            f"{pixetl_output_files_prefix}/{pixel_meaning}/{test_file}"
            for test_file in pixetl_test_files
        ]
        _check_s3_file_present(DATA_LAKE_BUCKET, test_files)

    _check_s3_file_present(
        TILE_CACHE_BUCKET, [f"{dataset}/{version}/{symbology['type']}/1/0/0.png"]
    )

    with patch("app.tasks.aws_tasks.get_cloudfront_client") as mock_client:
        mock_client.return_value = MockCloudfrontClient()
        for asset_id in new_asset_ids:
            await async_client.delete(f"/asset/{asset_id}")


def _check_s3_file_present(bucket, keys):
    s3_client = get_s3_client()

    for key in keys:
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
        except ClientError:
            raise AssertionError(f"Key {key} doesn't exist!")


def _delete_s3_files(bucket, prefix):
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in response.get("Contents", list()):
        print("Deleting", obj["Key"])
        s3_client.delete_object(Bucket=bucket, Key=obj["Key"])


@pytest.mark.asyncio
async def test_asset_stats(async_client):
    dataset = "test_asset_stats"
    version = "v1.0.0"

    pixetl_output_files_prefix = (
        f"{dataset}/{version}/raster/epsg-4326/90/27008/percent/"
    )
    _delete_s3_files(DATA_LAKE_BUCKET, pixetl_output_files_prefix)

    raster_version_payload = {
        "creation_options": {
            "source_type": "raster",
            "source_uri": [f"s3://{DATA_LAKE_BUCKET}/test/v1.1.1/raw/tiles.geojson"],
            "source_driver": "GeoTIFF",
            "data_type": "uint16",
            "pixel_meaning": "percent",
            "grid": "90/27008",
            "resampling": "nearest",
            "overwrite": True,
            "compute_histogram": True,
            "compute_stats": True,
            "no_data": 0,
        },
    }

    await create_default_asset(
        dataset,
        version,
        version_payload=raster_version_payload,
        async_client=async_client,
        execute_batch_jobs=True,
    )

    resp = await async_client.get(f"/dataset/{dataset}/{version}/assets")
    asset_id = resp.json()["data"][0]["asset_id"]

    asset_resp = await async_client.get(f"/asset/{asset_id}/stats")
    version_resp = await async_client.get(f"/dataset/{dataset}/{version}/stats")

    for resp in (asset_resp, version_resp):
        band_0 = resp.json()["data"]["bands"][0]
        assert band_0["min"] == 1.0
        assert band_0["max"] == 1.0
        assert band_0["mean"] == 1.0
        assert band_0["histogram"]["bin_count"] == 256
        assert band_0["histogram"]["value_count"][255] == 0
        assert resp.json()["data"]["bands"][0]["histogram"]["value_count"][0] == 10000


@pytest.mark.asyncio
async def test_asset_stats_no_histo(async_client):
    dataset = "test_asset_stats_no_histo"
    version = "v1.0.0"

    pixetl_output_files_prefix = (
        f"{dataset}/{version}/raster/epsg-4326/90/27008/percent/"
    )
    _delete_s3_files(DATA_LAKE_BUCKET, pixetl_output_files_prefix)

    raster_version_payload = {
        "creation_options": {
            "source_type": "raster",
            "source_uri": [f"s3://{DATA_LAKE_BUCKET}/test/v1.1.1/raw/tiles.geojson"],
            "source_driver": "GeoTIFF",
            "data_type": "uint16",
            "pixel_meaning": "percent",
            "grid": "90/27008",
            "resampling": "nearest",
            "overwrite": True,
            "compute_histogram": False,
            "compute_stats": True,
            "no_data": 0,
        },
    }

    await create_default_asset(
        dataset,
        version,
        version_payload=raster_version_payload,
        async_client=async_client,
        execute_batch_jobs=True,
    )

    resp = await async_client.get(f"/dataset/{dataset}/{version}/assets")
    asset_id = resp.json()["data"][0]["asset_id"]

    asset_resp = await async_client.get(f"/asset/{asset_id}/stats")
    version_resp = await async_client.get(f"/dataset/{dataset}/{version}/stats")

    for resp in (asset_resp, version_resp):
        assert resp.json()["data"]["bands"][0]["min"] == 1.0
        assert resp.json()["data"]["bands"][0]["max"] == 1.0
        assert resp.json()["data"]["bands"][0]["mean"] == 1.0
        assert resp.json()["data"]["bands"][0].get("histogram", None) is None


@pytest.mark.asyncio
async def test_asset_extent(async_client):
    dataset = "test_asset_extent"
    version = "v1.0.0"

    pixetl_output_files_prefix = (
        f"{dataset}/{version}/raster/epsg-4326/90/27008/percent/"
    )
    _delete_s3_files(DATA_LAKE_BUCKET, pixetl_output_files_prefix)

    raster_version_payload = {
        "creation_options": {
            "source_type": "raster",
            "source_uri": [f"s3://{DATA_LAKE_BUCKET}/test/v1.1.1/raw/tiles.geojson"],
            "source_driver": "GeoTIFF",
            "data_type": "uint16",
            "pixel_meaning": "percent",
            "grid": "90/27008",
            "resampling": "nearest",
            "overwrite": True,
            "compute_histogram": False,
            "compute_stats": False,
            "no_data": 0,
        },
    }

    await create_default_asset(
        dataset,
        version,
        version_payload=raster_version_payload,
        async_client=async_client,
        execute_batch_jobs=True,
    )

    expected_coords = [
        [[0.0, 90.0], [90.0, 90.0], [90.0, 0.0], [0.0, 0.0], [0.0, 90.0]]
    ]

    resp = await async_client.get(f"/dataset/{dataset}/{version}/assets")
    asset_id = resp.json()["data"][0]["asset_id"]

    resp = await async_client.get(f"/asset/{asset_id}/extent")
    # print(f"ASSET EXTENT RESP: {json.dumps(resp.json(), indent=2)}")
    assert (
        resp.json()["data"]["features"][0]["geometry"]["coordinates"] == expected_coords
    )

    resp = await async_client.get(f"/dataset/{dataset}/{version}/extent")
    # print(f"VERSION EXTENT RESP: {json.dumps(resp.json(), indent=2)}")
    assert (
        resp.json()["data"]["features"][0]["geometry"]["coordinates"] == expected_coords
    )


@pytest.mark.asyncio
async def test_asset_extent_stats_empty(async_client):
    dataset = "test_asset_extent_stats_empty"
    version = "v1.0.0"

    pixetl_output_files_prefix = (
        f"{dataset}/{version}/raster/epsg-4326/90/27008/percent/"
    )
    _delete_s3_files(DATA_LAKE_BUCKET, pixetl_output_files_prefix)

    raster_version_payload = {
        "creation_options": {
            "source_type": "raster",
            "source_uri": [f"s3://{DATA_LAKE_BUCKET}/test/v1.1.1/raw/tiles.geojson"],
            "source_driver": "GeoTIFF",
            "data_type": "uint16",
            "pixel_meaning": "percent",
            "grid": "90/27008",
            "resampling": "nearest",
            "overwrite": True,
            "compute_histogram": False,
            "compute_stats": False,
            "no_data": 0,
        },
    }

    await create_default_asset(
        dataset,
        version,
        version_payload=raster_version_payload,
        async_client=async_client,
        execute_batch_jobs=True,
    )

    resp = await async_client.get(f"/dataset/{dataset}/{version}/assets")
    asset_id = resp.json()["data"][0]["asset_id"]

    # # Update the extent fields of the asset to be None to simulate
    # # older assets in the DB
    async with ContextEngine("WRITE"):
        _ = await update_asset(asset_id, extent=None)

    # Verify that hitting the stats and extent endpoint for such assets
    # yields data=None rather than a 500
    resp = await async_client.get(f"/asset/{asset_id}/extent")
    assert resp.status_code == 200
    assert resp.json()["data"] is None
    resp = await async_client.get(f"/dataset/{dataset}/{version}/extent")
    assert resp.status_code == 200
    assert resp.json()["data"] is None

    resp = await async_client.get(f"/asset/{asset_id}/stats")
    assert resp.status_code == 200
    assert resp.json()["data"] is None
    resp = await async_client.get(f"/dataset/{dataset}/{version}/stats")
    assert resp.status_code == 200
    assert resp.json()["data"] is None
