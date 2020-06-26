from datetime import datetime
from typing import Any, Awaitable, Callable, Coroutine, Dict, List, Optional
from uuid import UUID

from fastapi.logger import logger

from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.jobs import Job
from ..utils.aws import get_batch_client


async def execute(
    jobs: List[Job],
    callback: Callable[
        [Optional[UUID], Dict[str, Any]], Coroutine[Any, Any, Awaitable[None]]
    ],
) -> ChangeLog:

    try:
        scheduled_jobs = await schedule(jobs, callback)
        print(f"SCHEDULED JOBS: {scheduled_jobs}")
    except RecursionError:
        status = "failed"
        message = "Failed to schedule batch jobs"
        detail = None
    else:
        status = "pending"
        message = "Successfully scheduled batch jobs"
        detail = f"Scheduled jobs: {scheduled_jobs}"
    return ChangeLog(
        date_time=datetime.now(), status=status, message=message, detail=detail
    )


async def schedule(
    jobs: List[Job],
    callback: Callable[
        [Optional[UUID], Dict[str, Any]], Coroutine[Any, Any, Awaitable[None]]
    ],
) -> Dict[str, UUID]:
    """

    Submit multiple batch jobs at once. Submitted batch jobs can depend on each other.
    Dependent jobs need to be listed in `dependent_jobs`
    and must have a `parents` attribute with the parent job names.

    """

    scheduled_jobs = dict()

    # first schedule all independent jobs
    for job in jobs:
        if not job.parents:
            scheduled_jobs[job.job_name] = submit_batch_job(job)
            await callback(
                scheduled_jobs[job.job_name],
                {
                    "date_time": datetime.now(),
                    "status": "pending",
                    "message": f"Scheduled job {job.job_name}",
                    "detail": f"Job ID: {scheduled_jobs[job.job_name]}",
                },
            )

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
                    {"jobId": str(scheduled_jobs[parent]), "type": "SEQUENTIAL"}
                    for parent in job.parents  # type: ignore
                ]
                scheduled_jobs[job.job_name] = submit_batch_job(job, depends_on)
                await callback(
                    scheduled_jobs[job.job_name],
                    {
                        "date_time": datetime.now(),
                        "status": "pending",
                        "message": f"Scheduled job {job.job_name}",
                        "detail": f"Job ID: {scheduled_jobs[job.job_name]}, parents: {depends_on}",
                    },
                )

        i += 1
        if i > 10:
            await callback(
                None,
                {
                    "date_time": datetime.now(),
                    "status": "failed",
                    "message": "Too many retries while scheduling jobs. Abort.",
                    "detail": f"Failed to schedule jobs {[job.job_name for job in jobs if job.job_name not in scheduled_jobs]}",
                },
            )
            raise RecursionError("Too many retries while scheduling jobs. Abort.")

    return scheduled_jobs


# async def poll_jobs(
#     job_ids: List[str], callback: Callable[[Dict[str, Any]], Awaitable[None]]
# ) -> str:
#     client = get_batch_client()
#     failed_jobs: Set[str] = set()
#     completed_jobs: Set[str] = set()
#     pending_jobs: Set[str] = set(job_ids)
#
#     while True:
#         response = client.describe_jobs(
#             jobs=list(pending_jobs.difference(completed_jobs))
#         )
#
#         for job in response["jobs"]:
#             print(
#                 f"Container for job {job['jobId']} exited with status {job['status']}"
#             )
#             if job["status"] == "SUCCEEDED":
#                 print(f"Container for job {job['jobId']} succeeded")
#                 await callback(
#                     {
#                         "date_time": datetime.now(),
#                         "status": "success",
#                         "message": f"Successfully completed job {job['jobName']}",
#                         "detail": None,
#                     }
#                 )
#                 completed_jobs.add(job["jobId"])
#             if job["status"] == "FAILED":
#                 print(f"Container for job {job['jobId']} failed")
#                 await callback(
#                     {
#                         "date_time": datetime.now(),
#                         "status": "failed",
#                         "message": f"Job {job['jobName']} failed during asset creation",
#                         "detail": job.get("statusReason", None),
#                     }
#                 )
#                 failed_jobs.add(job["jobId"])
#
#         if completed_jobs == set(job_ids):
#             await callback(
#                 {
#                     "date_time": datetime.now(),
#                     "status": "success",
#                     "message": "Successfully completed all scheduled batch jobs for asset creation",
#                     "detail": None,
#                 }
#             )
#             return "saved"
#         elif failed_jobs:
#             await callback(
#                 {
#                     "date_time": datetime.now(),
#                     "status": "failed",
#                     "message": "Job failures occurred during asset creation",
#                     "detail": None,
#                 }
#             )
#             return "failed"
#
#         sleep(POLL_WAIT_TIME)


def submit_batch_job(
    job: Job, depends_on: Optional[List[Dict[str, Any]]] = None
) -> UUID:
    """Submit job to AWS Batch."""
    client = get_batch_client()

    if depends_on is None:
        depends_on = list()

    payload = {
        "jobName": job.job_name,
        "jobQueue": job.job_queue,
        "dependsOn": depends_on,
        "jobDefinition": job.job_definition,
        "containerOverrides": {
            "command": job.command,
            "vcpus": job.vcpus,
            "memory": job.memory,
            "environment": job.environment,
        },
        "retryStrategy": {"attempts": job.attempts},
        "timeout": {"attemptDurationSeconds": job.attempt_duration_seconds},
    }

    logger.info(f"Submit batch job with payload: {payload}")

    response = client.submit_job(
        jobName=job.job_name,
        jobQueue=job.job_queue,
        dependsOn=depends_on,
        jobDefinition=job.job_definition,
        containerOverrides={
            "command": job.command,
            "vcpus": job.vcpus,
            "memory": job.memory,
            "environment": job.environment,
        },
        retryStrategy={"attempts": job.attempts},
        timeout={"attemptDurationSeconds": job.attempt_duration_seconds},
    )

    return UUID(response["jobId"])
