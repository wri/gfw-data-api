import json
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi.logger import logger

from ..errors import TooManyRetriesError
from ..models.enum.change_log import ChangeLogStatus
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.jobs import Job
from ..utils.aws import get_batch_client

BATCH_DEPENDENCY_LIMIT = 14
OOM_ERROR = "OutOfMemoryError: Container killed due to memory usage"


async def execute(
    jobs: List[Job],
) -> ChangeLog:
    try:
        scheduled_jobs = await schedule(jobs)
    except TooManyRetriesError as e:
        status = ChangeLogStatus.failed
        message = e.message
        detail = e.detail
    else:
        status = ChangeLogStatus.pending
        message = "Successfully scheduled batch jobs"
        detail = json.dumps(
            [
                {"job_name": job_name, "job_id": str(job_id)}
                for job_name, job_id in scheduled_jobs.items()
            ]
        )
    return ChangeLog(
        date_time=datetime.now(), status=status, message=message, detail=detail
    )


async def schedule(jobs: List[Job]) -> Dict[str, UUID]:
    """Submit multiple batch jobs at once.

    Submitted batch jobs can depend on each other. Dependent jobs need
    to be listed in `dependent_jobs` and must have a `parents` attribute
    with the parent job names.
    """

    # Since we currently use a dictionary with the job name as key,
    # we can't support scheduling multiple identically-named jobs
    # at the same time.
    if len(set([job.job_name for job in jobs])) != len(jobs):
        raise ValueError(
            "Can't schedule multiple jobs with the same name at the same time"
        )

    scheduled_jobs = dict()

    # first schedule all independent jobs
    for job in jobs:
        if not job.parents:
            scheduled_jobs[job.job_name] = submit_batch_job(job)
            await job.callback(
                task_id=scheduled_jobs[job.job_name],
                change_log=ChangeLog(
                    date_time=datetime.now(),
                    status=ChangeLogStatus.pending,
                    message=f"Scheduled job {job.job_name}",
                    detail=f"Job ID: {scheduled_jobs[job.job_name]}",
                ),
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
                await job.callback(
                    task_id=scheduled_jobs[job.job_name],
                    change_log=ChangeLog(
                        date_time=datetime.now(),
                        status=ChangeLogStatus.pending,
                        message=f"Scheduled job {job.job_name}",
                        detail=f"Job ID: {scheduled_jobs[job.job_name]}, parents: {depends_on}",
                    ),
                )

        i += 1
        if i > 10:
            raise TooManyRetriesError(
                message="Too many retries while scheduling jobs. Aborting.",
                detail=f"Failed to schedule job {[job.job_name for job in jobs if job.job_name not in scheduled_jobs]} ",
            )

    return scheduled_jobs


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
        "retryStrategy": {
            "attempts": job.attempts,
            "evaluateOnExit": [
                # Retry when our spot instance gets recalled
                {"onStatusReason": "Host EC2*", "action": "RETRY"},
                # Retry when a process has been killed involuntarily
                # This is likely due to OOM, and report_status.sh will
                # halve NUM_PROCESSES for retry
                {"onExitCode": "137", "action": "RETRY"},
                # Catch OOM situations which somehow didn't exit with 137
                {"onReason": OOM_ERROR, "action": "RETRY"},
                # Otherwise exit
                {"onReason": "*", "action": "EXIT"},
            ],
        },
        "timeout": {"attemptDurationSeconds": job.attempt_duration_seconds},
        "tags": {"Job": "Data-API Batch Job", "Dataset": job.dataset},
    }

    logger.info(f"Submitting batch job with payload: {payload}")

    response = client.submit_job(**payload)

    return UUID(response["jobId"])
