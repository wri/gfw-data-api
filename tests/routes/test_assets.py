from time import sleep

import boto3
import pytest
import requests
from botocore.exceptions import ClientError
from mock import patch

from app.crud import tasks
from app.settings.globals import AWS_REGION, DATA_LAKE_BUCKET
from tests.utils import create_default_asset, poll_jobs


@pytest.mark.asyncio
async def test_assets(async_client):
    """Basic tests of asset endpoint behavior."""
    # Add a dataset, version, and default asset
    dataset = "test"
    version = "v20200626"

    with patch("fastapi.BackgroundTasks.add_task", return_value=None):
        try:
            _ = await async_client.delete(f"/dataset/{dataset}/{version}")
            _ = await async_client.delete(f"/dataset/{dataset}")
        except Exception:
            pass

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

    # Clean up
    with patch("fastapi.BackgroundTasks.add_task", return_value=None):
        try:
            _ = await async_client.delete(f"/dataset/{dataset}/{version}")
            _ = await async_client.delete(f"/dataset/{dataset}")
        except Exception:
            pass


@pytest.mark.asyncio
async def test_auxiliary_raster_asset(async_client, batch_client, httpd):
    """"""
    _, logs = batch_client

    # Add a dataset, version, and default asset
    dataset = "test_auxiliary_raster_asset"
    version = "v1.0.0"
    primary_grid = "90/27008"
    auxiliary_grid = "90/9984"

    with patch("fastapi.BackgroundTasks.add_task", return_value=None):
        try:
            _ = await async_client.delete(f"/dataset/{dataset}/{version}")
            _ = await async_client.delete(f"/dataset/{dataset}")
        except Exception:
            pass

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
        "is_latest": True,
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
    # But sleep a moment for any stragglers to come in
    sleep(3)
    requests.delete(f"http://localhost:{httpd.server_port}")

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

    # Delete the asset or PostgreSQL throws an error downgrading
    # all the migrations.
    _ = await async_client.delete(f"/asset/{asset_id}")


@pytest.mark.asyncio
async def test_auxiliary_vector_asset(async_client, batch_client, httpd):
    """"""
    _, logs = batch_client

    # Add a dataset, version, and default asset
    dataset = "test_vector"
    version = "v1.1.1"

    with patch("fastapi.BackgroundTasks.add_task", return_value=None):
        try:
            _ = await async_client.delete(f"/dataset/{dataset}/{version}")
            _ = await async_client.delete(f"/dataset/{dataset}")
        except Exception:
            pass

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
    # But sleep a moment for any stragglers to come in
    sleep(3)
    requests.delete(f"http://localhost:{httpd.server_port}")

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
    # But sleep a moment for any stragglers to come in
    sleep(3)
    requests.delete(f"http://localhost:{httpd.server_port}")

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

    s3_client = boto3.client(
        "s3", region_name=AWS_REGION, endpoint_url="http://motoserver:5000"
    )

    pixetl_output_files = [
        f"{dataset}/{version}/raster/epsg-3857/zoom_0/date_conf/geotiff/extent.geojson",
        f"{dataset}/{version}/raster/epsg-3857/zoom_0/date_conf/geotiff/tiles.geojson",
        f"{dataset}/{version}/raster/epsg-3857/zoom_0/date_conf/geotiff/000R_000C.tif",
        f"{dataset}/{version}/raster/epsg-3857/zoom_0/intensity/geotiff/extent.geojson",
        f"{dataset}/{version}/raster/epsg-3857/zoom_0/intensity/geotiff/tiles.geojson",
        f"{dataset}/{version}/raster/epsg-3857/zoom_0/intensity/geotiff/000R_000C.tif",
        f"{dataset}/{version}/raster/epsg-3857/zoom_0/rgb_encoded/geotiff/000R_000C.tif",
    ]

    for key in pixetl_output_files:
        s3_client.delete_object(Bucket=DATA_LAKE_BUCKET, Key=key)

    print("FINISHED CLEANING UP")

    raster_version_payload = {
        "is_latest": True,
        "creation_options": {
            "source_type": "raster",
            "source_uri": [f"s3://{DATA_LAKE_BUCKET}/test/v1.1.1/raw/tiles.geojson"],
            "source_driver": "GeoTIFF",
            "data_type": "uint16",
            "no_data": 0,
            "pixel_meaning": "date_conf",
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

    # Verify that the asset and version are in state "saved"
    version_resp = await async_client.get(f"/dataset/{dataset}/{version}")
    assert version_resp.json()["data"]["status"] == "saved"

    asset_resp = await async_client.get(f"/asset/{default_asset_id}")
    assert asset_resp.json()["data"]["status"] == "saved"

    # Flush requests list so we're starting fresh
    requests.delete(f"http://localhost:{httpd.server_port}")

    # Add a tile cache asset based on the raster tile set
    asset_payload = {
        "asset_type": "Raster tile cache",
        "is_managed": True,
        "creation_options": {
            "source_asset_id": default_asset_id,
            "min_zoom": 0,
            "max_zoom": 14,
            "max_static_zoom": 2,
            "symbology": {"color_map": {"type": "date_conf_intensity"}},
        },
        "metadata": {},
    }

    create_asset_resp = await async_client.post(
        f"/dataset/{dataset}/{version}/assets", json=asset_payload
    )
    resp_json = create_asset_resp.json()
    assert resp_json["status"] == "success"
    assert resp_json["data"]["status"] == "pending"

    tile_cache_asset_id = resp_json["data"]["asset_id"]

    # Check the status of all tasks (except the default one which has already been checked)
    task_ids = []
    assets_response = await async_client.get(f"/dataset/{dataset}/{version}/assets")
    for asset in assets_response.json()["data"]:
        asset_id = asset["asset_id"]
        if asset_id != default_asset_id:
            tasks_resp = await async_client.get(f"/asset/{asset_id}/tasks")
            for task in tasks_resp.json()["data"]:
                task_ids += [task["task_id"]]
                print(f"Adding task id: {task['task_id']}")

    print(f"All found task ids: {task_ids}")

    # Wait until all batch jobs are done.
    status = await poll_jobs(task_ids, logs=logs, async_client=async_client)
    assert status == "saved"

    # Finally, make sure the raster tile cache asset is in "saved" state
    asset_resp = await async_client.get(f"/asset/{tile_cache_asset_id}")
    assert asset_resp.json()["data"]["status"] == "saved"

    # Oh, and make sure all files got uploaded to S3 along the way
    for key in pixetl_output_files:
        try:
            s3_client.head_object(Bucket=DATA_LAKE_BUCKET, Key=key)
        except ClientError:
            raise AssertionError(f"Key {key} doesn't exist!")

    # FIXME: Make sure files made it into the tile cache bucket as well!
