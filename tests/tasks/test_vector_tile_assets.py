import json

import httpx
import pytest
from mock import patch

from app.models.enum.assets import AssetType
from app.settings.globals import DATA_LAKE_BUCKET, TILE_CACHE_BUCKET
from app.utils.aws import get_s3_client

from .. import BUCKET, PORT, SHP_NAME
from ..utils import check_tasks_status, create_default_asset, poll_jobs
from . import MockECSClient


@pytest.mark.asyncio
@patch("app.tasks.aws_tasks.get_ecs_client")  # TODO use moto client
async def test_vector_tile_asset(ecs_client, batch_client, async_client):
    _, logs = batch_client
    ecs_client.return_value = MockECSClient()
    ############################
    # Setup test
    ############################

    dataset = "test"
    source = SHP_NAME

    version = "v1.1.1"
    input_data = {
        "creation_options": {
            "source_type": "vector",
            "source_uri": [f"s3://{BUCKET}/{source}"],
            "source_driver": "GeoJSON",
            "create_dynamic_vector_tile_cache": True,
        },
        "metadata": {},
    }

    await create_default_asset(
        dataset,
        version,
        version_payload=input_data,
        async_client=async_client,
        logs=logs,
        execute_batch_jobs=True,
        skip_dataset=False,
    )

    ### Create static tile cache asset
    httpx.delete(f"http://localhost:{PORT}")

    input_data = {
        "asset_type": "Static vector tile cache",
        "is_managed": True,
        "creation_options": {
            "min_zoom": 0,
            "max_zoom": 9,
            "tile_strategy": "discontinuous",
            "layer_style": [
                {
                    "id": dataset,
                    "paint": {"fill-color": "#9c9c9c", "fill-opacity": 0.8},
                    "source-layer": dataset,
                    "source": dataset,
                    "type": "fill",
                }
            ],
        },
    }

    response = await async_client.post(
        f"/dataset/{dataset}/{version}/assets", json=input_data
    )
    print(response.json())
    assert response.status_code == 202
    asset_id = response.json()["data"]["asset_id"]

    # get tasks id from change log and wait until finished
    response = await async_client.get(f"/asset/{asset_id}/change_log")

    assert response.status_code == 200
    tasks = json.loads(response.json()["data"][-1]["detail"])
    task_ids = [task["job_id"] for task in tasks]
    print(task_ids)

    # make sure, all jobs completed
    status = await poll_jobs(task_ids, logs=logs, async_client=async_client)
    assert status == "saved"

    response = await async_client.get(f"/dataset/{dataset}/{version}/assets")
    assert response.status_code == 200

    # there should be 4 assets now (geodatabase table, dynamic vector tile cache, ndjson and static vector tile cache)
    assert len(response.json()["data"]) == 4

    # there should be 10 files on s3 including the root.json and VectorTileServer files
    s3_client = get_s3_client()
    resp = s3_client.list_objects_v2(
        Bucket=TILE_CACHE_BUCKET, Prefix=f"{dataset}/{version}/default/"
    )
    print(resp)
    assert resp["KeyCount"] == 10

    response = await async_client.get(
        f"/dataset/{dataset}/{version}/assets?asset_type=ndjson"
    )
    assert response.status_code == 200
    assert len(response.json()["data"]) == 1
    asset_id = response.json()["data"][0]["asset_id"]

    # Check if file is in data lake
    resp = s3_client.list_objects_v2(
        Bucket=DATA_LAKE_BUCKET, Prefix=f"{dataset}/{version}/vector/"
    )
    print(resp)
    assert resp["KeyCount"] == 1

    response = await async_client.delete(f"/asset/{asset_id}")
    assert response.status_code == 200

    # Check if file was deleted
    resp = s3_client.list_objects_v2(
        Bucket=DATA_LAKE_BUCKET, Prefix=f"{dataset}/{version}/vector/"
    )
    print(resp)
    assert resp["KeyCount"] == 0

    ###########
    # 1x1 Grid
    ###########
    ### Create static tile cache asset
    httpx.delete(f"http://localhost:{PORT}")

    input_data = {
        "asset_type": "1x1 grid",
        "is_managed": True,
        "creation_options": {},
    }

    response = await async_client.post(
        f"/dataset/{dataset}/{version}/assets", json=input_data
    )

    print(response.json())
    assert response.status_code == 202
    asset_id = response.json()["data"]["asset_id"]

    # get tasks id from change log and wait until finished
    response = await async_client.get(f"/asset/{asset_id}/change_log")

    assert response.status_code == 200
    tasks = json.loads(response.json()["data"][-1]["detail"])
    task_ids = [task["job_id"] for task in tasks]
    print(task_ids)

    # make sure, all jobs completed
    status = await poll_jobs(task_ids, logs=logs, async_client=async_client)
    assert status == "saved"

    response = await async_client.get(f"/dataset/{dataset}/{version}/assets")
    assert response.status_code == 200

    # there should be 4 assets now (geodatabase table, dynamic vector tile cache and static vector tile cache (already deleted ndjson)
    assert len(response.json()["data"]) == 4

    # Check if file is in tile cache
    resp = s3_client.list_objects_v2(
        Bucket=DATA_LAKE_BUCKET, Prefix=f"{dataset}/{version}/vector/"
    )
    print(resp)
    assert resp["KeyCount"] == 1

    response = await async_client.delete(f"/asset/{asset_id}")
    print(response.json())
    assert response.status_code == 200

    # Check if file was deleted
    resp = s3_client.list_objects_v2(
        Bucket=DATA_LAKE_BUCKET, Prefix=f"{dataset}/{version}/vector/"
    )
    print(resp)
    assert resp["KeyCount"] == 0

    ###########
    # Vector file export
    ###########

    asset_types = [AssetType.shapefile, AssetType.geopackage]
    for asset_type in asset_types:
        response = await async_client.get(f"/dataset/{dataset}/{version}/assets")
        current_asset_count = len(response.json()["data"])

        httpx.delete(f"http://localhost:{PORT}")

        input_data = {
            "asset_type": asset_type,
            "is_managed": True,
            "creation_options": {},
        }

        response = await async_client.post(
            f"/dataset/{dataset}/{version}/assets", json=input_data
        )

        print(response.json())
        assert response.status_code == 202
        asset_id = response.json()["data"]["asset_id"]

        await check_tasks_status(async_client, logs, [asset_id])

        response = await async_client.get(f"/dataset/{dataset}/{version}/assets")
        assert response.status_code == 200

        # there should be one more asset than before this test
        assert len(response.json()["data"]) == current_asset_count + 1

        # Check if file is in data lake
        resp = s3_client.list_objects_v2(
            Bucket=DATA_LAKE_BUCKET, Prefix=f"{dataset}/{version}/vector/"
        )
        print(resp)
        assert resp["KeyCount"] == 1

        response = await async_client.delete(f"/asset/{asset_id}")
        print(response.json())
        assert response.status_code == 200

        # Check if file was deleted
        resp = s3_client.list_objects_v2(
            Bucket=DATA_LAKE_BUCKET, Prefix=f"{dataset}/{version}/vector/"
        )
        print(resp)
        assert resp["KeyCount"] == 0
