from unittest.mock import patch

import boto3
import pytest

import app
import app.tasks.batch as batch
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


@pytest.mark.asyncio
async def test_batch_scheduler(batch_client):
    async def callback(message):
        pass

    _, logs = batch_client

    batch.POLL_WAIT_TIME = 1
    job1 = PostgresqlClientJob(
        job_name="job1",
        command=["add_gfw_fields.sh", "-d", "test", "-v", "v1.0.0"],
        environment=writer_secrets,
    )
    job2 = GdalPythonImportJob(
        job_name="job2",
        command=[
            "create_vector_schema.sh",
            "-d",
            "test",
            "-v",
            "v1.0.0",
            "-s",
            "source_uri.shp",
            "-l",
            "source_uri",
        ],
        environment=writer_secrets,
    )
    job3 = GdalPythonExportJob(
        job_name="job3",
        command=["echo", "test"],
        parents=[job1.job_name, job2.job_name],
        environment=writer_secrets,
    )
    job4 = TileCacheJob(
        job_name="job4",
        command=["echo", "test"],
        parents=[job3.job_name],
        environment=writer_secrets,
    )

    log = await batch.execute([job1, job2, job3, job4], callback)
    assert log.status == "failed"
    #
    # resp = logs.describe_log_streams(
    #     logGroupName="/aws/batch/job"
    # )
    #
    # for stream in resp["logStreams"]:
    #     ls_name = stream["logStreamName"]
    #
    #     stream_resp = logs.get_log_events(
    #         logGroupName="/aws/batch/job", logStreamName=ls_name
    #     )
    #
    #     print(f"-------- LOGS FROM {ls_name} --------")
    #     for event in stream_resp["events"]:
    #         assert event[
    #                    "message"] == "PSQL: ALTER TABLE .. Add GFW columns\npsql: could not connect to server: No such file or directory\nIs the server running locally and accepting\nconnections on Unix domain socket \"/var/run/postgresql/.s.PGSQL.5432\"?"
    #         # print(event["message"])


# import os
# os.environ["AWS_ACCESS_KEY_ID"] = "testing"
# os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"  # pragma: allowlist secret
# os.environ["AWS_SECURITY_TOKEN"] = "testing"
# os.environ["AWS_SESSION_TOKEN"] = "testing"
#


def test_s3(moto_s3):
    # import boto3
    # boto3.client("s3", region_name="us-east-1", endpoint_url="http://motoserver:5000") #

    client = app.utils.aws.get_s3_client()

    client.create_bucket(Bucket="my_bucket_name")
    more_binary_data = b"Here we have some more data"

    client.put_object(
        Body=more_binary_data,
        Bucket="my_bucket_name",
        Key="my/key/including/anotherfilename.txt",
    )
    assert moto_s3.called
    assert moto_s3.call_count == 1
