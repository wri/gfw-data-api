from uuid import uuid4

from app.models.pydantic.jobs import Job
from app.tasks import callback_constructor


def test_jobs_model():

    callback = callback_constructor(uuid4())

    job = Job(
        dataset="test",
        job_name="test",
        job_queue="test",
        job_definition="test",
        command=["1"],
        environment=[{"name": "TEST", "value": "TEST"}],
        vcpus=1,
        memory=2,
        attempts=1,
        attempt_duration_seconds=1,
        parents=None,
        callback=callback,
    )

    assert job.environment == [
        {"name": "TEST", "value": "TEST"},
        {"name": "CORES", "value": "1"},
        {"name": "MAX_MEM", "value": "2"},
    ]

    job.vcpus = 45
    assert job.environment == [
        {"name": "TEST", "value": "TEST"},
        {"name": "CORES", "value": "45"},
        {"name": "MAX_MEM", "value": "2"},
    ]

    job.memory = 100
    assert job.environment == [
        {"name": "TEST", "value": "TEST"},
        {"name": "CORES", "value": "45"},
        {"name": "MAX_MEM", "value": "100"},
    ]
