import uuid
from time import sleep
from typing import Any, Dict, List, Set

import requests
from mock import patch

from app.crud import tasks
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
            print_logs(logs)
            await check_callbacks(job_ids, async_client)
            return status

        sleep(1)


async def check_callbacks(task_ids, async_client=None):
    get_resp = requests.get(f"http://localhost:{PORT}")
    req_list = get_resp.json()["requests"]

    print("REQUEST", req_list)
    print("TASKS", task_ids)
    assert len(req_list) == len(task_ids)

    task_path = [f"/task/{taskid}" for taskid in task_ids]
    req_paths = list()
    for i, req in enumerate(req_list):
        print("#############")
        print(req)
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
            print(response.json())
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
