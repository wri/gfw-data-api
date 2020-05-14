import pytest

from app.models.pydantic.job import (
    PostgresqlClientJob,
    GdalPythonImportJob,
    GdalPythonExportJob,
    TileCacheJob,
)
import app.tasks.batch as batch


@pytest.mark.asyncio
async def test_batch_scheduler(batch_client):
    batch.POLL_WAIT_TIME = 1

    job1 = PostgresqlClientJob(
        job_name="job1", command=["add_gfw_fields.sh", "-d", "test", "-v", "v1.0.0"]
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
    )
    job3 = GdalPythonExportJob(
        job_name="job3",
        command=["echo", "test"],
        parents=[job1.job_name, job2.job_name],
    )
    job4 = TileCacheJob(
        job_name="job4", command=["echo", "test"], parents=[job3.job_name]
    )

    success = await batch.execute([job1, job2, job3, job4], lambda x: x)
    assert success
