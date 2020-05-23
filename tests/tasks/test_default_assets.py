import os
from typing import List
from uuid import UUID

import boto3
import pytest
from sqlalchemy.sql.ddl import CreateSchema

from app.application import ContextEngine, db
from app.crud import datasets, versions
from app.models.orm.geostore import Geostore
from app.settings.globals import AWS_REGION, READER_USERNAME
from app.tasks.default_assets import create_default_asset

GEOJSON_NAME = "test.geojson"
GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "..", "fixtures", GEOJSON_NAME)
BUCKET = "test-bucket"


@pytest.mark.asyncio
async def test_vector_source_asset(client, batch_client):
    # TODO: define what a callback should do
    async def callback(message):
        pass

    _, logs = batch_client

    # Upload file to mocked S3 bucket
    s3_client = boto3.client(
        "s3", region_name=AWS_REGION, endpoint_url="http://motoserver:5000"
    )

    s3_client.create_bucket(Bucket=BUCKET)
    s3_client.upload_file(GEOJSON_PATH, BUCKET, GEOJSON_NAME)

    dataset = "test"
    version = "v1.1.1"
    input_data = {
        "source_type": "vector",
        "source_uri": [f"s3://{BUCKET}/{GEOJSON_NAME}"],
        "creation_options": {"src_driver": "GeoJSON", "zipped": False},
        "metadata": {},
    }

    # Create dataset and version records
    async with ContextEngine("PUT"):
        await datasets.create_dataset(dataset)
        await db.status(CreateSchema(dataset))
        await db.status(f"GRANT USAGE ON SCHEMA {dataset} TO {READER_USERNAME};")
        await db.status(
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {dataset} GRANT SELECT ON TABLES TO {READER_USERNAME};"
        )
        await versions.create_version(dataset, version, **input_data)

    # To start off, version should be in status "pending"
    row = await versions.get_version(dataset, version)
    assert row.status == "pending"

    # Create default asset in mocked BATCH
    await create_default_asset(
        dataset, version, input_data, None, callback,
    )

    # Get the logs in case something went wrong
    _print_logs(logs)

    # If everything worked, version should be set to "saved"
    row = await versions.get_version(dataset, version)
    assert row.status == "saved"

    # There should be a table called "test"."v1.1.1" with one row
    async with ContextEngine("GET"):
        count = await db.scalar(db.text('SELECT count(*) FROM test."v1.1.1"'))
    assert count == 1

    # The geometry should also be accessible via geostore
    async with ContextEngine("GET"):
        rows: List[Geostore] = await Geostore.query.gino.all()

    assert len(rows) == 1
    assert rows[0].gfw_geostore_id == UUID("b9faa657-34c9-96d4-fce4-8bb8a1507cb3")


@pytest.mark.asyncio
async def test_table_source_asset(client, batch_client):
    raise NotImplementedError("Still in the works")


def _print_logs(logs):
    resp = logs.describe_log_streams(logGroupName="/aws/batch/job")

    for stream in resp["logStreams"]:
        ls_name = stream["logStreamName"]

        stream_resp = logs.get_log_events(
            logGroupName="/aws/batch/job", logStreamName=ls_name
        )

        print(f"-------- LOGS FROM {ls_name} --------")
        for event in stream_resp["events"]:
            print(event["message"])
