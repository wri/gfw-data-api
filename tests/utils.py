import json
import os
import tempfile
import uuid
from time import sleep
from typing import Any, Dict, List, Set

import boto3
import httpx
import numpy
import rasterio
from affine import Affine
from mock import patch
from rasterio.crs import CRS

from app.crud import tasks
from app.settings.globals import AWS_REGION, DATA_LAKE_BUCKET
from app.utils.aws import get_batch_client
from tests import BUCKET, PORT, SHP_NAME
from tests.tasks import MockECSClient

generic_dataset_payload = {
    "metadata": {
        "title": "string",
        "subtitle": "string",
        "function": "string",
        "resolution": "string",
        "geographic_coverage": "string",
        "source": "string",
        "update_frequency": "string",
        "cautions": "string",
        "license": "string",
        "overview": "string",
        "citation": "string",
        "tags": ["string"],
        "data_language": "string",
        "key_restrictions": "string",
        "scale": "string",
        "added_date": "2020-06-25",
        "why_added": "string",
        "other": "string",
        "learn_more": "string",
    }
}

generic_version_payload = {
    "metadata": {},
    "creation_options": {
        "source_driver": "ESRI Shapefile",
        "source_type": "vector",
        "source_uri": [f"s3://{BUCKET}/{SHP_NAME}"],
    },
}


async def create_dataset(
    dataset_name, async_client, payload: Dict[str, Any] = generic_dataset_payload
) -> Dict[str, Any]:
    resp = await async_client.put(f"/dataset/{dataset_name}", json=payload)
    # print(f"CREATE_DATASET_RESPONSE: {resp.json()}")
    assert resp.json()["status"] == "success"
    return resp.json()["data"]


async def create_version(
    dataset, version, async_client, payload: Dict[str, Any] = generic_version_payload
) -> Dict[str, Any]:

    resp = await async_client.put(f"/dataset/{dataset}/{version}", json=payload)
    # print(f"CREATE_VERSION RESPONSE: {resp.json()}")
    assert resp.json()["status"] == "success"

    return resp.json()["data"]


async def create_default_asset(
    dataset,
    version,
    dataset_payload: Dict[str, Any] = generic_dataset_payload,
    version_payload: Dict[str, Any] = generic_version_payload,
    async_client=None,
    logs=None,
    execute_batch_jobs=False,
    skip_dataset=False,
) -> Dict[str, Any]:
    # Create dataset, version, and default asset records.
    # The default asset is created automatically when the version is created.

    if not skip_dataset:
        await create_dataset(dataset, async_client, dataset_payload)

    if execute_batch_jobs:
        await create_version(dataset, version, async_client, version_payload)
    else:
        with patch("app.tasks.batch.submit_batch_job", side_effect=generate_uuid):
            await create_version(dataset, version, async_client, version_payload)

    # Verify that a record for the default asset was created
    resp = await async_client.get(f"/dataset/{dataset}/{version}/assets")
    # print(f"ASSET RESP: {resp.json()}")
    assert len(resp.json()["data"]) == 1
    assert resp.json()["status"] == "success"

    asset = resp.json()["data"][0]

    if execute_batch_jobs:
        # wait until batch jobs are done.
        tasks_rows = await tasks.get_tasks(asset["asset_id"])
        task_ids = [str(task.task_id) for task in tasks_rows]
        status = await poll_jobs(task_ids, logs=logs, async_client=async_client)
        assert status == "saved"

    return asset


def generate_uuid(*args, **kwargs) -> uuid.UUID:
    return uuid.uuid4()


async def poll_jobs(job_ids: List[str], logs=None, async_client=None) -> str:
    client = get_batch_client()
    failed_jobs: Set[str] = set()
    completed_jobs: Set[str] = set()
    pending_jobs: Set[str] = set(job_ids)
    status = None

    while True:
        response = client.describe_jobs(
            jobs=list(pending_jobs.difference(completed_jobs))
        )

        for job in response["jobs"]:
            print(
                f"Container for job {job['jobId']} exited with status {job['status']}"
            )
            if job["status"] == "SUCCEEDED":
                print(f"Container for job {job['jobId']} succeeded")

                completed_jobs.add(job["jobId"])
            if job["status"] == "FAILED":
                print(f"Container for job {job['jobId']} failed")

                failed_jobs.add(job["jobId"])

        if completed_jobs == set(job_ids):
            status = "saved"
        elif failed_jobs:
            status = "failed"

        if status:
            # print_logs(logs)
            await check_callbacks(job_ids, async_client)
            return status

        sleep(1)


async def check_callbacks(task_ids, async_client=None):
    get_resp = httpx.get(f"http://localhost:{PORT}")
    req_list = get_resp.json()["requests"]

    # print("REQUESTS", req_list)
    # print("TASKS", task_ids)
    assert len(req_list) == len(task_ids)

    task_path = [f"/task/{taskid}" for taskid in task_ids]
    req_paths = list()
    for i, req in enumerate(req_list):
        # print("#############")
        # print(req)
        req_paths.append(req["path"])
        assert req["body"]["change_log"][0]["status"] == "success"
        if async_client:
            await forward_request(async_client, req)

    # Order of task might vary if running in parallel.
    # We only check if URLs were correct
    assert all(elem in task_path for elem in req_paths)


async def forward_request(async_client, request):
    # ecs_client.return_value = MockECSClient()

    client_request = {
        "PATCH": async_client.patch,
        "PUT": async_client.put,
        "POST": async_client.post,
    }

    try:
        # TODO: use moto
        with patch("app.tasks.aws_tasks.get_ecs_client", return_value=MockECSClient()):
            response = await client_request[request["method"]](
                request["path"], json=request["body"]
            )
            # print(response.json())
            assert response.status_code == 200
    except KeyError:
        raise NotImplementedError(
            f"Forwarding method {request['method']} not implemented"
        )


def print_logs(logs):
    if logs:
        resp = logs.describe_log_streams(logGroupName="/aws/batch/job")

        for stream in resp["logStreams"]:
            ls_name = stream["logStreamName"]

            stream_resp = logs.get_log_events(
                logGroupName="/aws/batch/job", logStreamName=ls_name
            )

            print(f"-------- LOGS FROM {ls_name} --------")
            for event in stream_resp["events"]:
                print(event["message"])


async def check_tasks_status(async_client, logs, asset_ids) -> None:
    tasks = list()

    for asset_id in asset_ids:
        # get tasks id from change log and wait until finished
        response = await async_client.get(f"/asset/{asset_id}/change_log")

        assert response.status_code == 200

        resp_logs = response.json()["data"]
        for log in resp_logs:
            if log["message"] == "Successfully scheduled batch jobs":
                tasks += json.loads(log["detail"])

    task_ids = [task["job_id"] for task in tasks]

    # make sure, all jobs completed
    status = await poll_jobs(task_ids, logs=logs, async_client=async_client)
    assert status == "saved"


def upload_fake_data(dtype, dtype_name, no_data, prefix):
    s3_client = boto3.client(
        "s3", region_name=AWS_REGION, endpoint_url="http://motoserver:5000"
    )

    data_file_name = "0000000000-0000000000.tif"

    tiles_geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [[1.0, 1.0], [2.0, 1.0], [2.0, 0.0], [1.0, 0.0], [1.0, 1.0]]
                    ],
                },
                "properties": {
                    "name": f"/vsis3/{DATA_LAKE_BUCKET}/{prefix}/{data_file_name}"
                },
            }
        ],
    }

    dataset_profile = {
        "driver": "GTiff",
        "dtype": dtype,
        "nodata": no_data,
        "count": 1,
        "width": 100,
        "height": 100,
        "blockxsize": 100,
        "blockysize": 100,
        "crs": CRS.from_epsg(4326),
        # 0.003332345971563981 is the pixel size of 90/27008
        "transform": Affine(0.003332345971563981, 0, 1, 0, -0.003332345971563981, 1),
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        full_tiles_path = f"{os.path.join(tmpdir, 'tiles.geojson')}"

        with open(full_tiles_path, "w") as dst:
            dst.write(json.dumps(tiles_geojson))
        s3_client.upload_file(
            full_tiles_path,
            DATA_LAKE_BUCKET,
            f"{prefix}/tiles.geojson",
        )

        full_data_file_path = f"{os.path.join(tmpdir, data_file_name)}"
        with rasterio.Env():
            with rasterio.open(full_data_file_path, "w", **dataset_profile) as dst:
                dummy_data = numpy.ones((100, 100), dtype)
                dst.write(dummy_data.astype(dtype), 1)
        s3_client.upload_file(
            full_data_file_path,
            DATA_LAKE_BUCKET,
            f"{prefix}/{data_file_name}",
        )
