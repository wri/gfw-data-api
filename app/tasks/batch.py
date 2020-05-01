from typing import Any, Dict, List, Optional

import boto3

from app.models.pydantic.jobs import Job

client = boto3.client("batch")


def scheduler(
    independent_jobs: List[Job], dependent_jobs: Optional[List[Job]] = None
) -> None:
    """
    Submit multiple batch jobs at once. Submitted batch jobs can depend on each other.
    Dependent jobs need to be listed in `dependent_jobs`
    and must have a `parents` attribute with the parent job names
    """

    scheduled_jobs = dict()

    # first schedule all independent jobs
    for job in independent_jobs:
        scheduled_jobs[job.job_name] = submit_batch_job(job)

    # then retry to schedule all dependent jobs
    # until all parent job are scheduled or max retry is reached
    i = 0

    while dependent_jobs:

        _jobs: List[Job] = dependent_jobs
        dependent_jobs = list()

        for job in _jobs:
            try:
                depends_on = [
                    {"jobId": scheduled_jobs[parent], "type": "SEQUENTIAL"}
                    for parent in job.parents  # type: ignore
                ]
                scheduled_jobs[job.job_name] = submit_batch_job(job, depends_on)
            except KeyError:
                dependent_jobs.append(job)
    i += 1
    if i > 7:
        raise RecursionError("Too many retries while scheduling jobs. Aboard.")


def submit_batch_job(
    job: Job, depends_on: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """
    Submit job to AWS Batch
    """

    if depends_on is None:
        depends_on = list()

    response = client.submit_job(
        jobName=job.job_name,
        jobQueue=job.job_queue,
        dependsOn=depends_on,
        jobDefinition=job.job_definition,
        containerOverrides={
            "command": job.command,
            "vcpus": job.vcpus,
            "memory": job.memory,
        },
        retryStrategy={"attempts": job.attempts},
        timeout={"attemptDurationSeconds": job.attempt_duration_seconds},
    )

    return response["jobId"]
