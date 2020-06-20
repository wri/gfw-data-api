import os
from unittest.mock import patch

import boto3
import pytest

import app
import app.tasks.batch as batch
from app.application import ContextEngine
from app.crud import datasets, versions
from app.models.pydantic.jobs import (
    GdalPythonExportJob,
    GdalPythonImportJob,
    PostgresqlClientJob,
    TileCacheJob,
)
from app.settings.globals import (
    AWS_REGION,
    WRITER_DBNAME,
    WRITER_HOST,
    WRITER_PASSWORD,
    WRITER_PORT,
    WRITER_USERNAME,
)

writer_secrets = [
    {"name": "PGPASSWORD", "value": str(WRITER_PASSWORD)},
    {"name": "PGHOST", "value": WRITER_HOST},
    {"name": "PGPORT", "value": WRITER_PORT},
    {"name": "PGDATABASE", "value": WRITER_DBNAME},
    {"name": "PGUSER", "value": WRITER_USERNAME},
]

GEOJSON_NAME = "test.geojson"
GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "..", "fixtures", GEOJSON_NAME)
BUCKET = "test-bucket"


@pytest.mark.asyncio
async def test_batch_scheduler(batch_client):
    async def callback(message):
        pass

    _, logs = batch_client

    batch.POLL_WAIT_TIME = 1

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

    async with ContextEngine("WRITE"):
        await datasets.create_dataset(dataset)
        await versions.create_version(dataset, version, **input_data)

    job1 = PostgresqlClientJob(
        job_name="job1",
        command=["test_mock_s3_awscli.sh", "-s", f"s3://{BUCKET}/{GEOJSON_NAME}"],
        environment=writer_secrets,
    )
    job2 = GdalPythonImportJob(
        job_name="job2",
        command=[
            "test_mock_s3_ogr2ogr.sh",
            "-d",
            "test",
            "-v",
            "v1.0.0",
            "-s",
            f"s3://{BUCKET}/{GEOJSON_NAME}",
            "-l",
            "test",
            "-f",
            GEOJSON_NAME,
        ],
        environment=writer_secrets,
        parents=[job1.job_name],
    )
    job3 = GdalPythonExportJob(
        job_name="job3",
        command=[
            "test_mock_s3_ogr2ogr.sh",
            "-d",
            "test",
            "-v",
            "v1.0.0",
            "-s",
            f"s3://{BUCKET}/{GEOJSON_NAME}",
            "-l",
            "test",
            "-f",
            GEOJSON_NAME,
        ],
        environment=writer_secrets,
        parents=[job2.job_name],
    )
    job4 = TileCacheJob(
        job_name="job4",
        command=["test_mock_s3_awscli.sh", "-s", f"s3://{BUCKET}/{GEOJSON_NAME}"],
        environment=writer_secrets,
        parents=[job3.job_name],
    )

    log = await batch.execute([job1, job2, job3, job4], callback)
    assert log.status == "saved"


#
# def test_s3(moto_s3):
#     # import boto3
#     # boto3.client("s3", region_name="us-east-1", endpoint_url="http://motoserver:5000") #
#
#     client = app.utils.aws.get_s3_client()
#
#     client.create_bucket(Bucket="my_bucket_name")
#     more_binary_data = b"Here we have some more data"
#
#     client.put_object(
#         Body=more_binary_data,
#         Bucket="my_bucket_name",
#         Key="my/key/including/anotherfilename.txt",
#     )
#     assert moto_s3.called
#     assert moto_s3.call_count == 1
