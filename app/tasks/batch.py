from typing import Any, Dict, List, Optional, Callable, Awaitable, Set
from time import sleep
from datetime import datetime
import os

from app.models.pydantic.job import Job

POLL_WAIT_TIME = 30
BATCH_CLIENT = None
REGION = os.environ.get("REGION", "us-east-1")


def execute(jobs: List[Job], callback: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
    scheduled_jobs = schedule(jobs)

    return poll_jobs(scheduled_jobs.values(), callback)


def get_batch_client():
    import boto3

    global BATCH_CLIENT
    if BATCH_CLIENT is None:
        BATCH_CLIENT = boto3.client("batch", region_name=REGION)
    return BATCH_CLIENT


def schedule(jobs: List[Job]) -> Dict[str, str]:
    """
    Submit multiple batch jobs at once. Submitted batch jobs can depend on each other.
    Dependent jobs need to be listed in `dependent_jobs`
    and must have a `parents` attribute with the parent job names
    """

    scheduled_jobs = dict()

    # first schedule all independent jobs
    for job in jobs:
        if not job.parents:
            scheduled_jobs[job.job_name] = submit_batch_job(job)

    if not scheduled_jobs:
        raise ValueError(
            "No independent jobs in list, can't start scheduling process due to missing dependencies"
        )

    # then retry to schedule all dependent jobs
    # until all parent job are scheduled or max retry is reached
    i = 0

    while len(jobs) != len(scheduled_jobs):
        for job in jobs:
            if (
                job.job_name not in scheduled_jobs
                and job.parents is not None
                and all([parent in scheduled_jobs for parent in job.parents])
            ):
                depends_on = [
                    {"jobId": scheduled_jobs[parent], "type": "SEQUENTIAL"}
                    for parent in job.parents  # type: ignore
                ]
                scheduled_jobs[job.job_name] = submit_batch_job(job, depends_on)

        i += 1
        if i > 7:
            raise RecursionError("Too many retries while scheduling jobs. Aboard.")

    return scheduled_jobs


def poll_jobs(
    job_ids: List[str], callback: Callable[[Dict[str, Any]], Awaitable[None]]
) -> bool:

    client = get_batch_client()
    failed_jobs: Set[str] = set()
    completed_jobs: Set[str] = set()
    pending_jobs: Set[str] = set(job_ids)

    while True:
        response = client.describe_jobs(jobs=list(pending_jobs.difference(completed_jobs)))
        print(response)

        for job in response['jobs']:
            if job['status'] == 'SUCCEEDED':
                callback(
                    {
                        "datetime": datetime.now(),
                        "status": "success",
                        "message": f"Successfully completed job {job['jobName']}",
                        "detail": None,
                    }
                )
                completed_jobs.add(job["jobId"])
            if job["status"] == "FAILED":
                callback(
                    {
                        "datetime": datetime.now(),
                        "status": "failed",
                        "message": f"Job {job['jobName']} failed during asset creation",
                        "detail": job["statusReason"],
                    }
                )
                failed_jobs.add(job["jobId"])

        if completed_jobs == set(job_ids):
            callback({
                "datetime": datetime.now(),
                "status": "success",
                "message": f"Successfully completed all scheduled batch jobs for asset creation",
                "detail": None,
            })
            return True
        elif failed_jobs:
            callback(
                {
                    "datetime": datetime.now(),
                    "status": "failed",
                    "message": f"Job failures occurred during asset creation",
                    "detail": None,
                }
            )
            return False

        sleep(POLL_WAIT_TIME)


def submit_batch_job(
    job: Job, depends_on: Optional[List[Dict[str, Any]]] = None
) -> str:
    """
    Submit job to AWS Batch
    """
    client = get_batch_client()

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
