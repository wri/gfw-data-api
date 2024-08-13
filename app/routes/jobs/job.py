"""Jobs represent long running analysis tasks. Certain APIs, like querying like
a list, will return immediately with a job_id. You can poll the job until it's
complete, and a download like will be provided.

Jobs are only saved for 90 days.
"""
import json
from typing import Any, Dict
from uuid import UUID

import botocore
from fastapi import APIRouter, HTTPException, Path
from fastapi.logger import logger
from fastapi.responses import ORJSONResponse

from ...models.pydantic.user_job import UserJob, UserJobResponse
from ...settings.globals import RASTER_ANALYSIS_STATE_MACHINE_ARN
from ...utils.aws import get_sfn_client
from ..datasets import _get_presigned_url_from_path

router = APIRouter()


@router.get(
    "/{job_id}",
    response_class=ORJSONResponse,
    tags=["Jobs"],
    response_model=UserJobResponse,
)
async def get_job(*, job_id: UUID = Path(...)) -> UserJobResponse:
    """Get job status.

    Jobs expire after 90 days.
    """
    try:
        job = await _get_user_job(job_id)
        return UserJobResponse(data=job)
    except botocore.exceptions.ClientError as e:
        raise HTTPException(status_code=404, detail=str(e))


async def _get_user_job(job_id: UUID) -> UserJob:
    execution = await _get_sfn_execution(job_id)

    if execution["status"] == "SUCCEEDED":
        output = (
            json.loads(execution["output"]) if execution["output"] is not None else None
        )

        if output["status"] == "success":
            download_link = await _get_presigned_url_from_path(
                output["data"]["download_link"]
            )
            failed_geometries_link = None
        elif output["status"] == "partial_success":
            download_link = await _get_presigned_url_from_path(
                output["data"]["download_link"]
            )
            failed_geometries_link = await _get_presigned_url_from_path(
                output["data"]["failed_geometries_link"]
            )
        elif output["status"] == "failed":
            download_link = None
            failed_geometries_link = await _get_presigned_url_from_path(
                output["data"]["failed_geometries_link"]
            )
        else:
            logger.error(f"Analysis service returned an unexpected response: {output}")
            return UserJob(
                job_id=job_id,
                status="failed",
                download_link=None,
                failed_geometries_link=None,
                progress="0%",
            )

        return UserJob(
            job_id=job_id,
            status=output["status"],
            download_link=download_link,
            failed_geometries_link=failed_geometries_link,
            progress=await _get_progress(execution),
        )

    elif execution["status"] == "RUNNING":
        return UserJob(
            job_id=job_id,
            status="pending",
            download_link=None,
            failed_geometries_link=None,
            progress=await _get_progress(execution),
        )
    else:
        return UserJob(
            job_id=job_id,
            status="failed",
            download_link=None,
            failed_geometries_link=None,
            progress=None,
        )


async def _get_sfn_execution(job_id: UUID) -> Dict[str, Any]:
    execution_arn = f"{RASTER_ANALYSIS_STATE_MACHINE_ARN.replace('stateMachine', 'execution')}:{str(job_id)}"
    execution = get_sfn_client().describe_execution(executionArn=execution_arn)
    return execution


async def _get_progress(execution: Dict[str, Any]) -> str:
    map_run = await _get_map_run(execution)
    success_ratio = map_run["itemCounts"]["succeeded"] / map_run["itemCounts"]["total"]
    return f"{round(success_ratio * 100)}%"


async def _get_map_run(execution: Dict[str, Any]) -> Dict[str, Any]:
    map_runs = get_sfn_client().list_map_runs(executionArn=execution["executionArn"])
    map_run_arn = map_runs["mapRuns"][0]["mapRunArn"]
    map_run = get_sfn_client().describe_map_run(mapRunArn=map_run_arn)
    return map_run
