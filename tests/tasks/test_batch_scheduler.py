from .batch_client import batch_client

from app.models.pydantic.job import Job
import app.tasks.batch as batch


def test_batch_scheduler(batch_client):
    batch.POLL_WAIT_TIME = 1

    job1 = Job(job_name="job1", command=["/bin/bash"])
    job2 = Job(job_name="job2", command=["/bin/bash"])
    job3 = Job(job_name="job3", command=["/bin/bash"], parents=[job1.job_name, job2.job_name])

    success = batch.execute([job1, job2, job3], lambda x: x)
    assert(success)

