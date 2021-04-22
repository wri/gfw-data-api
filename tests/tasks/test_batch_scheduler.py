import pytest

import app.tasks.batch as batch
from app.crud import tasks
from app.models.pydantic.jobs import (
    GdalPythonExportJob,
    GdalPythonImportJob,
    PostgresqlClientJob,
    TileCacheJob,
)
from app.settings.globals import (
    WRITER_DBNAME,
    WRITER_HOST,
    WRITER_PASSWORD,
    WRITER_PORT,
    WRITER_USERNAME,
)
from app.tasks import callback_constructor

from .. import BUCKET, GEOJSON_NAME
from ..utils import poll_jobs
from . import create_asset

writer_secrets = [
    {"name": "PGPASSWORD", "value": str(WRITER_PASSWORD)},
    {"name": "PGHOST", "value": WRITER_HOST},
    {"name": "PGPORT", "value": WRITER_PORT},
    {"name": "PGDATABASE", "value": WRITER_DBNAME},
    {"name": "PGUSER", "value": WRITER_USERNAME},
]


@pytest.mark.skip(
    reason="This is just to make sure that the batch scheduler works. Only run when in doubt."
)
@pytest.mark.asyncio
async def test_batch_scheduler(batch_client, httpd, async_client):

    _, logs = batch_client
    httpd_port = httpd.server_port

    ############################
    # Setup test
    ############################

    job_env = writer_secrets + [
        {"name": "STATUS_URL", "value": f"http://app_test:{httpd_port}/tasks"}
    ]

    batch.POLL_WAIT_TIME = 1

    dataset = "test"
    version = "v1.1.1"
    input_data = {
        "source_type": "vector",
        "source_uri": [f"s3://{BUCKET}/{GEOJSON_NAME}"],
        "creation_options": {"src_driver": "GeoJSON", "zipped": False},
        "metadata": {},
    }

    new_asset = await create_asset(
        dataset, version, "Database table", "s3://path/to/file", input_data
    )
    callback = callback_constructor(new_asset.asset_id)
    ############################
    # Test if mocking batch jobs using the different environments works
    ############################

    job1 = PostgresqlClientJob(
        dataset=dataset,
        job_name="job1",
        command=["test_mock_s3_awscli.sh", "-s", f"s3://{BUCKET}/{GEOJSON_NAME}"],
        environment=job_env,
        callback=callback,
    )
    job2 = GdalPythonImportJob(
        dataset=dataset,
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
        environment=job_env,
        parents=[job1.job_name],
        callback=callback,
    )
    job3 = GdalPythonExportJob(
        dataset=dataset,
        job_name="job3",
        command=["test_mock_s3_awscli.sh", "-s", f"s3://{BUCKET}/{GEOJSON_NAME}"],
        environment=job_env,
        parents=[job2.job_name],
        callback=callback,
    )
    job4 = TileCacheJob(
        dataset=dataset,
        job_name="job4",
        command=["test_mock_s3_awscli.sh", "-s", f"s3://{BUCKET}/{GEOJSON_NAME}"],
        environment=job_env,
        parents=[job3.job_name],
        callback=callback,
    )

    log = await batch.execute([job1, job2, job3, job4])
    assert log.status == "pending"

    tasks_rows = await tasks.get_tasks(new_asset.asset_id)

    task_ids = [str(task.task_id) for task in tasks_rows]

    # make sure, all jobs completed
    status = await poll_jobs(task_ids, logs=logs, async_client=async_client)
    assert status == "saved"
