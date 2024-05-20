import json
from unittest.mock import patch
from uuid import UUID

import httpx
import pytest
from botocore.exceptions import ClientError
from httpx import AsyncClient

from app.application import ContextEngine
from app.crud import tasks
from app.crud.assets import update_asset
from app.models.enum.creation_options import ColorMapType
from app.models.pydantic.jobs import GDAL2TilesJob, GDALDEMJob, PixETLJob
from app.settings.globals import TILE_CACHE_BUCKET
from app.tasks.utils import sanitize_batch_job_name
from app.utils.aws import get_s3_client
from tests import BUCKET, DATA_LAKE_BUCKET, SHP_NAME
from tests.conftest import FAKE_FLOAT_DATA_PARAMS, FAKE_INT_DATA_PARAMS
from tests.tasks import MockCloudfrontClient
from tests.utils import (
    asset_metadata,
    check_s3_file_present,
    check_tasks_status,
    create_dataset,
    create_default_asset,
    create_version,
    dataset_metadata,
    delete_s3_files,
    generate_uuid,
    poll_jobs,
)

s3_client = get_s3_client()


@pytest.mark.asyncio
async def test_assets(async_client):
    """Basic tests of asset endpoint behavior."""
    # Add a dataset, version, and default asset
    dataset = "test_assets"
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
async def test_assets_vector_source_max_parents(async_client):
    """Make sure that vector source assets with > 20 layers stay within AWS
    parents limit."""
    # Add a dataset, version, and default asset
    dataset = "test_vector_source_max_parents"
    version = "v20210310"

    vector_source_payload = {
        "creation_options": {
            "source_type": "vector",
            "source_uri": [f"s3://{BUCKET}/{SHP_NAME}"],
            "source_driver": "FileGDB",
            "layers": [
                "aus_plant",
                "chl_plant",
                "chn_plant",
                "civ_plant",
                "cmr_plant",
                "cod_plant",
                "col_plant",
                "cri_plant",
                "ecu_plant",
                "eu_plant",
                "gab_plant",
                "gha_plant",
                "gtm_plant",
                "hnd_plant",
                "idn_plant",
                "ind_plant",
                "jpn_plant",
                "ken_plant",
                "khm_plant",
                "kor_plant",
                "lbr_plant",
                "lka_plant",
                "mex_plant",
            ],
        },
    }

    await create_dataset(dataset, async_client, {"metadata": dataset_metadata})

    with patch(
        "app.tasks.batch.submit_batch_job", side_effect=generate_uuid
    ) as mock_submit:
        await create_version(
            dataset, version, async_client, payload=vector_source_payload
        )

    load_vector_data_jobs = list()
    for mock_call in mock_submit.call_args_list:
        job_obj = mock_call[0][0]
        if job_obj.parents is not None:
            assert len(job_obj.parents) <= 19
        if "load_vector_data_layer_" in job_obj.job_name:
            load_vector_data_jobs.append(job_obj)

    assert len(load_vector_data_jobs) == len(
        vector_source_payload["creation_options"]["layers"]
    )


@pytest.mark.asyncio
async def test_auxiliary_raster_asset(async_client, httpd, logs):
    """"""
    # Add a dataset, version, and default asset
    dataset = "test_auxiliary_raster_asset"
    version = "v1.8"
    primary_grid = "90/27008"
    auxiliary_grid = "90/9984"

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
            "source_uri": [
                f"s3://{DATA_LAKE_BUCKET}/{FAKE_INT_DATA_PARAMS['prefix']}/tiles.geojson"
            ],
            "source_driver": "GeoTIFF",
            "data_type": FAKE_INT_DATA_PARAMS["dtype_name"],
            "no_data": FAKE_INT_DATA_PARAMS["no_data"],
            "pixel_meaning": "percent",
            "grid": primary_grid,
            "resampling": "nearest",
            "overwrite": True,
            "subset": "90N_000E",
        }
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
            "data_type": FAKE_INT_DATA_PARAMS["dtype_name"],
            "pixel_meaning": "gfw_fid",
            "grid": auxiliary_grid,
            "resampling": "nearest",
            "overwrite": True,
            "subset": "90N_000E",
            "no_data": FAKE_INT_DATA_PARAMS["no_data"],
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
async def test_rasterize_vector_asset(async_client: AsyncClient, batch_client, httpd):
    """"""
    _, logs = batch_client

    # Add a dataset, version, and default asset
    dataset = "test_vector"
    version = "v1.1.1"
    grid = "10/40000"

    pixetl_output_files = [
        f"{dataset}/{version}/raster/epsg-4326/{grid}/gfw_fid/gdal-geotiff/extent.geojson",
        f"{dataset}/{version}/raster/epsg-4326/{grid}/gfw_fid/geotiff/extent.geojson",
        f"{dataset}/{version}/raster/epsg-4326/{grid}/gfw_fid/gdal-geotiff/tiles.geojson",
        f"{dataset}/{version}/raster/epsg-4326/{grid}/gfw_fid/geotiff/tiles.geojson",
        f"{dataset}/{version}/raster/epsg-4326/{grid}/gfw_fid/gdal-geotiff/60N_010E.tif",
        f"{dataset}/{version}/raster/epsg-4326/{grid}/gfw_fid/geotiff/60N_010E.tif",
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
        "is_managed": True,
        "creation_options": {
            "data_type": FAKE_INT_DATA_PARAMS["dtype"],
            "pixel_meaning": "gfw_fid",
            "grid": grid,
            "resampling": "nearest",
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


symbology_checks = [
    {
        "wm_tile_set_assets": [
            f"date_conf_{ColorMapType.date_conf_intensity}",
            f"intensity_{ColorMapType.date_conf_intensity}",
            ColorMapType.date_conf_intensity,
        ],
        "symbology": {"type": ColorMapType.date_conf_intensity},
    },
    {
        "wm_tile_set_assets": [
            f"date_conf_{ColorMapType.year_intensity}",
            f"intensity_{ColorMapType.year_intensity}",
            ColorMapType.year_intensity,
        ],
        "symbology": {"type": ColorMapType.year_intensity},
    },
    {
        "wm_tile_set_assets": [
            f"date_conf_{ColorMapType.gradient}",
            ColorMapType.gradient,
        ],
        "symbology": {
            "type": ColorMapType.gradient,
            "colormap": {
                1: {"red": 255, "green": 0, "blue": 0},
                40000: {"red": 0, "green": 0, "blue": 255},
            },
        },
    },
    {
        "wm_tile_set_assets": [
            f"date_conf_{ColorMapType.discrete}",
            ColorMapType.discrete,
        ],
        "symbology": {
            "type": ColorMapType.discrete,
            "colormap": {
                20100: {"red": 255, "green": 0, "blue": 0},
                30100: {"red": 0, "green": 0, "blue": 255},
            },
        },
    },
    {
        "wm_tile_set_assets": [
            f"date_conf_{ColorMapType.gradient_intensity}",
            f"colormap_{ColorMapType.gradient_intensity}",
            f"intensity_{ColorMapType.gradient_intensity}",
            ColorMapType.gradient_intensity,
        ],
        "symbology": {
            "type": ColorMapType.gradient_intensity,
            "colormap": {
                1: {"red": 255, "green": 0, "blue": 0},
                40000: {"red": 0, "green": 0, "blue": 255},
            },
        },
    },
]


@pytest.mark.skip("Disabling for a few days while replacements are made")
@pytest.mark.parametrize("checks", symbology_checks)
@pytest.mark.asyncio
async def test_raster_tile_cache_asset(checks, async_client, batch_client, httpd):
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
            "source_uri": [
                f"s3://{DATA_LAKE_BUCKET}/{FAKE_INT_DATA_PARAMS['prefix']}/tiles.geojson"
            ],
            "source_driver": "GeoTIFF",
            "data_type": FAKE_INT_DATA_PARAMS["dtype_name"],
            "no_data": FAKE_INT_DATA_PARAMS["no_data"],
            "pixel_meaning": pixel_meaning,
            "grid": primary_grid,
            "resampling": "nearest",
            "overwrite": True,
        },
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

    # Flush requests list so we're starting fresh
    httpx.delete(f"http://localhost:{httpd.server_port}")

    await _test_raster_tile_cache(
        dataset,
        version,
        default_asset_id,
        async_client,
        logs,
        **checks,
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

    delete_s3_files(DATA_LAKE_BUCKET, pixetl_output_files_prefix)
    delete_s3_files(TILE_CACHE_BUCKET, f"{dataset}/{version}")

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
        "metadata": asset_metadata,
    }

    old_assets_resp = await async_client.get(f"/dataset/{dataset}/{version}/assets")
    old_asset_ids = set([asset["asset_id"] for asset in old_assets_resp.json()["data"]])

    create_asset_resp = await async_client.post(
        f"/dataset/{dataset}/{version}/assets", json=asset_payload
    )
    resp_json = create_asset_resp.json()
    print(f"CREATE TILE CACHE ASSET RESPONSE: {resp_json}")
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

    # Make sure the creation_options endpoint works (verify that the bug
    # with numeric keys in colormap is fixed, GTC-974):
    c_o_resp = await async_client.get(f"/asset/{tile_cache_asset_id}/creation_options")
    assert c_o_resp.json()["status"] == "success"

    # Check if file for all expected assets are present
    for pixel_meaning in wm_tile_set_assets:
        test_files = [
            f"{pixetl_output_files_prefix}/{pixel_meaning}/{test_file}"
            for test_file in pixetl_test_files
        ]
        check_s3_file_present(DATA_LAKE_BUCKET, test_files)

    # TODO: GTC-1090
    #  these tests should pass once ticket is resolved
    # somehow I didn't get the GDAL_ENV to work, short cut here.
    # s3_client.download_file(
    #     DATA_LAKE_BUCKET,
    #     f"test_raster_tile_cache_asset/v1.0.0/raster/epsg-3857/zoom_1/{symbology['type']}/geotiff/000R_000C.tif",
    #     "local_copy.tif",
    # )
    # with rasterio.open("local_copy.tif") as img:
    #     nodata_vals = img.nodatavals
    #     max_vals = [arr.max() for arr in img.read()]
    #
    # assert (
    #     3 <= len(nodata_vals) <= 4
    # ), f"File should have either 3 (RGB) or 4 (RGBA) bands. Band count: {len(nodata_vals)}"
    # assert all(
    #     val == 0 for val in nodata_vals
    # ), f"All no data values must be 0. Values: {nodata_vals}"
    # assert (
    #     max(max_vals) > 0
    # ), f"There should be at least one band value larger than 0. Values: {max_vals}"

    check_s3_file_present(
        TILE_CACHE_BUCKET, [f"{dataset}/{version}/{symbology['type']}/1/1/0.png"]
    )
    check_s3_file_present(
        TILE_CACHE_BUCKET, [f"{dataset}/{version}/{symbology['type']}/0/0/0.png"]
    )

    # TODO: GTC-1090
    #  these test should pass once ticket is resolved
    # # There should be no empty tiles for files with an alpha band
    # with pytest.raises(AssertionError):
    #         # This is an empty tile and should not exist
    #         check_s3_file_present(
    #             TILE_CACHE_BUCKET,
    #             [f"{dataset}/{version}/{symbology['type']}/1/0/0.png"],
    #         )
    #
    with patch("app.tasks.aws_tasks.get_cloudfront_client") as mock_client:
        mock_client.return_value = MockCloudfrontClient()
        for asset_id in new_asset_ids:
            await async_client.delete(f"/asset/{asset_id}")


@pytest.mark.hanging
@pytest.mark.asyncio
async def test_asset_stats(async_client):
    dataset = "test_asset_stats"
    version = "v1.0.0"

    pixetl_output_files_prefix = (
        f"{dataset}/{version}/raster/epsg-4326/90/27008/percent/"
    )
    delete_s3_files(DATA_LAKE_BUCKET, pixetl_output_files_prefix)

    raster_version_payload = {
        "creation_options": {
            "source_type": "raster",
            "source_uri": [
                f"s3://{DATA_LAKE_BUCKET}/{FAKE_INT_DATA_PARAMS['prefix']}/tiles.geojson"
            ],
            "source_driver": "GeoTIFF",
            "data_type": FAKE_INT_DATA_PARAMS["dtype_name"],
            "no_data": FAKE_INT_DATA_PARAMS["no_data"],
            "pixel_meaning": "percent",
            "grid": "90/27008",
            "resampling": "nearest",
            "overwrite": True,
            "compute_histogram": True,
            "compute_stats": True,
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
        assert band_0["min"] == 0.0
        assert band_0["max"] == 10000.0
        assert band_0.get("mean") is not None
        assert band_0["histogram"]["bin_count"] == 256


@pytest.mark.hanging
@pytest.mark.asyncio
async def test_asset_stats_no_histo(async_client):
    dataset = "test_asset_stats_no_histo"
    version = "v1.0.0"

    pixetl_output_files_prefix = (
        f"{dataset}/{version}/raster/epsg-4326/90/27008/percent/"
    )
    delete_s3_files(DATA_LAKE_BUCKET, pixetl_output_files_prefix)

    raster_version_payload = {
        "creation_options": {
            "source_type": "raster",
            "source_uri": [
                f"s3://{DATA_LAKE_BUCKET}/{FAKE_INT_DATA_PARAMS['prefix']}/tiles.geojson"
            ],
            "source_driver": "GeoTIFF",
            "data_type": FAKE_INT_DATA_PARAMS["dtype_name"],
            "no_data": FAKE_INT_DATA_PARAMS["no_data"],
            "pixel_meaning": "percent",
            "grid": "90/27008",
            "resampling": "nearest",
            "overwrite": True,
            "compute_histogram": False,
            "compute_stats": True,
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
        assert resp.json()["data"]["bands"][0].get("histogram") is None


@pytest.mark.hanging
@pytest.mark.asyncio
async def test_asset_extent(async_client):
    dataset = "test_asset_extent"
    version = "v1.0.0"

    pixetl_output_files_prefix = (
        f"{dataset}/{version}/raster/epsg-4326/90/27008/percent/"
    )
    delete_s3_files(DATA_LAKE_BUCKET, pixetl_output_files_prefix)

    raster_version_payload = {
        "creation_options": {
            "source_type": "raster",
            "source_uri": [
                f"s3://{DATA_LAKE_BUCKET}/{FAKE_INT_DATA_PARAMS['prefix']}/tiles.geojson"
            ],
            "source_driver": "GeoTIFF",
            "data_type": FAKE_INT_DATA_PARAMS["dtype_name"],
            "no_data": FAKE_INT_DATA_PARAMS["no_data"],
            "pixel_meaning": "percent",
            "grid": "90/27008",
            "resampling": "nearest",
            "overwrite": True,
            "compute_histogram": False,
            "compute_stats": False,
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
    assert (
        resp.json()["data"]["features"][0]["geometry"]["coordinates"] == expected_coords
    )

    resp = await async_client.get(f"/dataset/{dataset}/{version}/extent")
    assert (
        resp.json()["data"]["features"][0]["geometry"]["coordinates"] == expected_coords
    )


@pytest.mark.hanging
@pytest.mark.asyncio
async def test_asset_extent_stats_empty(async_client):
    dataset = "test_asset_extent_stats_empty"
    version = "v1.0.0"

    pixetl_output_files_prefix = (
        f"{dataset}/{version}/raster/epsg-4326/90/27008/percent/"
    )
    delete_s3_files(DATA_LAKE_BUCKET, pixetl_output_files_prefix)

    raster_version_payload = {
        "creation_options": {
            "source_type": "raster",
            "source_uri": [
                f"s3://{DATA_LAKE_BUCKET}/{FAKE_INT_DATA_PARAMS['prefix']}/tiles.geojson"
            ],
            "source_driver": "GeoTIFF",
            "data_type": FAKE_INT_DATA_PARAMS["dtype_name"],
            "no_data": FAKE_INT_DATA_PARAMS["no_data"],
            "pixel_meaning": "percent",
            "grid": "90/27008",
            "resampling": "nearest",
            "overwrite": True,
            "compute_histogram": False,
            "compute_stats": False,
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


@pytest.mark.hanging
@pytest.mark.asyncio
async def test_asset_float(async_client, batch_client, httpd):
    _, logs = batch_client

    dataset = "test_asset_float_no_data"
    version = "v1.0.0"
    pixel_meaning = "percent"

    pixetl_output_files_prefix = f"{dataset}/{version}/raster/epsg-3857/zoom_1/percent"
    delete_s3_files(DATA_LAKE_BUCKET, pixetl_output_files_prefix)

    raster_version_payload = {
        "creation_options": {
            "source_type": "raster",
            "source_uri": [
                f"s3://{DATA_LAKE_BUCKET}/{FAKE_FLOAT_DATA_PARAMS['prefix']}/tiles.geojson"
            ],
            "source_driver": "GeoTIFF",
            "data_type": FAKE_FLOAT_DATA_PARAMS["dtype_name"],
            "no_data": str(FAKE_FLOAT_DATA_PARAMS["no_data"]),
            "pixel_meaning": pixel_meaning,
            "grid": "90/27008",
            "resampling": "nearest",
            "compute_stats": True,
        }
    }

    asset = await create_default_asset(
        dataset,
        version,
        version_payload=raster_version_payload,
        async_client=async_client,
        execute_batch_jobs=True,
    )
    default_asset_id = asset["asset_id"]

    symbology = {
        "type": ColorMapType.gradient,
        "colormap": {
            -1: {"red": 255, "green": 0, "blue": 0},
            1: {"red": 0, "green": 0, "blue": 255},
        },
    }

    expected_scaled_symbology = {
        "type": "gradient",
        "colormap": {
            # Remember it's okay if these are negative (or greater than
            # uint16.max), they're based on the original breakpoints,
            # which could have been far above or below the
            # original data max and min
            "-32766.0": {"red": 255, "green": 0, "blue": 0},
            "98302.0": {"red": 0, "green": 0, "blue": 255},
        },
    }

    # Flush requests list so we're starting fresh
    httpx.delete(f"http://localhost:{httpd.server_port}")

    # Make the call to create a tile cache, but mock out actually
    # creating the jobs because 1. It takes a while and 2. Otherwise Pixetl hangs
    # when running this test locally
    tile_cache_levels = 2
    max_zoom_levels = tile_cache_levels + 1
    with patch(
        "app.tasks.batch.submit_batch_job", side_effect=generate_uuid
    ) as mock_submit:
        # Add a tile cache asset based on the raster tile set
        asset_payload = {
            "asset_type": "Raster tile cache",
            "is_managed": True,
            "creation_options": {
                "source_asset_id": default_asset_id,
                "min_zoom": 0,
                "max_zoom": max_zoom_levels,
                "max_static_zoom": tile_cache_levels - 1,
                "symbology": symbology,
                "implementation": symbology["type"],
            },
        }

        create_asset_resp = await async_client.post(
            f"/dataset/{dataset}/{version}/assets", json=asset_payload
        )
        resp_json = create_asset_resp.json()
        assert resp_json["status"] == "success"
        assert resp_json["data"]["status"] == "pending"

    # Now check the mock
    pixetl_jobs = dict()
    gdaldem_jobs = dict()
    gdal2tiles_jobs = dict()

    for mock_call in mock_submit.call_args_list:
        job_obj = mock_call[0][0]
        if isinstance(job_obj, PixETLJob):
            cmd = job_obj.command
            layer_def = None
            for i, arg in enumerate(cmd):
                if arg == "-j":
                    layer_def = json.loads(cmd[i + 1])
            assert layer_def is not None
            job = (
                layer_def["data_type"],
                layer_def["no_data"],
                layer_def.get("symbology"),
                job_obj.parents,
            )
            pixetl_jobs[job_obj.job_name] = job
        elif isinstance(job_obj, GDAL2TilesJob):
            gdal2tiles_jobs[job_obj.job_name] = job_obj.parents
        elif isinstance(job_obj, GDALDEMJob):
            cmd = job_obj.command
            no_data_value = None
            symbology = None
            for i, arg in enumerate(cmd):
                if arg == "-j":
                    symbology = json.loads(cmd[i + 1])
                if arg == "-n":
                    no_data_value = json.loads(cmd[i + 1])
            assert symbology is not None
            job = (
                symbology,
                no_data_value,
                job_obj.parents,
            )
            gdaldem_jobs[job_obj.job_name] = job
        else:
            raise Exception("Unknown job type found")

    assert pixetl_jobs == {
        sanitize_batch_job_name(f"{dataset}_{version}_{pixel_meaning}_gradient_{i}"): (
            "uint16",
            0,
            None,
            [
                sanitize_batch_job_name(
                    f"{dataset}_{version}_{pixel_meaning}_gradient_{i+1}"
                )
            ]
            if i < max_zoom_levels
            else None,
        )
        for i in range(0, max_zoom_levels + 1)
    }
    assert gdaldem_jobs == {
        sanitize_batch_job_name(f"{dataset}_{version}_gradient_{i}"): (
            expected_scaled_symbology,
            0,
            [
                sanitize_batch_job_name(
                    f"{dataset}_{version}_{pixel_meaning}_gradient_{i}"
                )
            ],
        )
        for i in range(0, max_zoom_levels + 1)
    }

    assert gdal2tiles_jobs == {
        f"generate_tile_cache_zoom_{i}": [
            sanitize_batch_job_name(f"{dataset}_{version}_gradient_{i}"),
            sanitize_batch_job_name(
                f"{dataset}_{version}_{pixel_meaning}_gradient_{i}"
            ),
        ]
        for i in range(0, tile_cache_levels)
    }

    # Make sure creation options are correctly parsed.
    all_asset_resp = await async_client.get(f"/dataset/{dataset}/{version}/assets")

    for asset in all_asset_resp.json()["data"]:
        asset_co_resp = await async_client.get(
            f"/asset/{asset['asset_id']}/creation_options"
        )
        assert asset_co_resp.status_code == 200


@pytest.mark.asyncio
async def test_raster_asset_payloads_vector_source(async_client):
    """Test creating various raster assets based on vector input."""

    # Add a dataset, version, and default asset
    dataset = "vector_test"
    version = "v20200626"

    #
    asset = await create_default_asset(
        dataset, version, async_client=async_client, execute_batch_jobs=False
    )
    asset_id = asset["asset_id"]

    # Since we did not actually execute the batch job, all tasks are still pending
    # We pretend that they succeeded so that we can continue creating assets
    get_resp = await async_client.get(f"/asset/{asset_id}/tasks")
    task_list = get_resp.json()["data"]
    for task in task_list:
        task_id = task["task_id"]
        patch_payload = {
            "change_log": [
                {
                    "date_time": "2020-06-25 14:30:00",
                    "status": "success",
                    "message": "Let's fake it",
                    "detail": "None",
                }
            ]
        }
        patch_resp = await async_client.patch(f"/task/{task_id}", json=patch_payload)
        assert patch_resp.json()["status"] == "success"

    create_asset_resp = await async_client.get(f"/asset/{asset_id}")
    assert create_asset_resp.json()["data"]["status"] == "saved"

    ######################
    # Create Raster Tile Set based on Vector source asset
    #######################

    # Try adding a non-default raster tile asset based on the default

    asset_payload = {
        "asset_type": "Raster tile set",
        "is_managed": True,
        "creation_options": {
            "pixel_meaning": "year",
            "data_type": "uint8",
            "nbits": 5,
            "no_data": 0,
            "rasterize_method": "value",
            "calc": "year",
            "order": "asc",
            "grid": "10/40000",
            "symbology": None,
        },
    }

    # Only checking if payload is accepted, nothing else
    with patch("app.tasks.batch.submit_batch_job", side_effect=generate_uuid):
        create_asset_resp = await async_client.post(
            f"/dataset/{dataset}/{version}/assets", json=asset_payload
        )
        resp_json = create_asset_resp.json()
        assert resp_json["status"] == "success"
