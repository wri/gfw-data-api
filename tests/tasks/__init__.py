import os
from datetime import datetime
from time import sleep
from typing import Any, Dict, List, Set

import requests
from sqlalchemy.sql.ddl import CreateSchema

from app.application import ContextEngine, db
from app.crud import assets, datasets, versions
from app.models.orm.assets import Asset as AssetORM
from app.settings.globals import READER_USERNAME
from app.utils.aws import get_batch_client

TSV_NAME = "test.tsv"
TSV_PATH = os.path.join(os.path.dirname(__file__), "..", "fixtures", TSV_NAME)

GEOJSON_NAME = "test.geojson"
GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "..", "fixtures", GEOJSON_NAME)

BUCKET = "test-bucket"
KEY = "KEY"
VALUE = "VALUE"


class MockS3Client(object):
    rules: List[Dict[str, Any]] = []

    def get_bucket_lifecycle_configuration(self, Bucket):
        return {"Rules": self.rules}

    def put_bucket_lifecycle_configuration(self, Bucket, LifecycleConfiguration):
        self.rules = LifecycleConfiguration["Rules"]
        return {
            "ResponseMetadata": {"...": "..."},
        }


class MockCloudfrontClient(object):
    def create_invalidation(self, DistributionId, InvalidationBatch):
        return {
            "Location": "string",
            "Invalidation": {
                "Id": "string",
                "Status": "string",
                "CreateTime": datetime.now(),
                "InvalidationBatch": InvalidationBatch,
            },
        }


async def poll_jobs(job_ids: List[str]) -> str:
    client = get_batch_client()
    failed_jobs: Set[str] = set()
    completed_jobs: Set[str] = set()
    pending_jobs: Set[str] = set(job_ids)

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

            return "saved"
        elif failed_jobs:

            return "failed"

        sleep(1)


def check_callbacks(task_ids, port):
    get_resp = requests.get(f"http://localhost:{port}")
    req_list = get_resp.json()["requests"]

    print("#############")
    print(len(req_list), req_list)
    print(len(task_ids), task_ids)
    assert len(req_list) == len(task_ids)

    task_path = [f"/tasks/{taskid}" for taskid in task_ids]
    req_paths = list()
    for i, req in enumerate(req_list):
        req_paths.append(req["path"])
        assert req["body"]["change_log"][0]["status"] == "success"

    # Order of task might vary if running in parallel.
    # We only check if URLs were correct
    assert all(elem in task_path for elem in req_paths)


async def create_version(dataset, version, input_data) -> None:
    # Create dataset and version records
    async with ContextEngine("WRITE"):
        await datasets.create_dataset(dataset)
        await db.status(CreateSchema(dataset))
        await db.status(f"GRANT USAGE ON SCHEMA {dataset} TO {READER_USERNAME};")
        await db.status(
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {dataset} GRANT SELECT ON TABLES TO {READER_USERNAME};"
        )
        await versions.create_version(dataset, version, **input_data)

    # Make sure everything we need is in place
    # To start off, version should be in status "pending"
    # and changelog should be an empty list
    # and dataset schema should exist
    row = await versions.get_version(dataset, version)
    assert row.status == "pending"
    assert row.change_log == []
    assert dataset == await db.scalar(
        f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{dataset}';"
    )


async def create_asset(dataset, version, asset_type, asset_uri, input_data) -> AssetORM:
    await create_version(dataset, version, input_data)
    # Create dataset and version records
    async with ContextEngine("WRITE"):
        new_asset = await assets.create_asset(
            dataset, version, asset_type=asset_type, asset_uri=asset_uri,
        )
    return new_asset
