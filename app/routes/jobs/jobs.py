"""Jobs represent the steps performed during asset creation.

You can view a single tasks or all tasks associated with as specific
asset. Only _service accounts_ can create or update tasks.
"""
from typing import Any, Dict
from uuid import UUID

import botocore
from fastapi import APIRouter, HTTPException, Path
from fastapi.responses import ORJSONResponse

from ...models.pydantic.responses import Response
from ...settings.globals import STATE_MACHINE_ARN
from ...utils.aws import get_sfn_client

router = APIRouter()


@router.get(
    "/{job_id}",
    response_class=ORJSONResponse,
    tags=["Jobs"],
    response_model=Response,
)
async def get_job(*, job_id: UUID = Path(...)) -> Response:
    """Get single tasks by task ID."""
    try:
        return await _get_sfn_job(job_id)
    except botocore.exceptions.ClientError as e:
        raise HTTPException(status_code=404, detail=str(e))


async def _get_sfn_job(job_id: UUID):
    execution_arn = (
        f"{STATE_MACHINE_ARN.replace('stateMachines', 'execution')}:{job_id}"
    )
    execution = get_sfn_client().describe_execution(execution_arn)

    if execution["status"] == "SUCCEEDED":
        return {
            "job_id": job_id,
            "status": execution["output"]["status"],
            "download_link": execution["output"]["download_link"],
            "progress": "100%",
        }
    elif execution["status"] == "PENDING":
        return {
            "job_id": job_id,
            "status": "pending",
            "download_link": None,
            "progress": await _get_progress(execution),
        }
    else:
        return {
            "job_id": job_id,
            "status": "failed",
            "download_link": None,
            "progress": "0%",
        }


async def _get_progress(execution: Dict[str, Any]) -> str:
    map_runs = get_sfn_client().list_map_runs(executionArn=execution["executionArn"])
    map_run = get_sfn_client().describe_map_run(mapRunArn=map_runs[0]["mapRunArn"])

    sucess_ratio = map_run["itemCounts"]["succeeded"] / map_run["itemCounts"]["total"]
    return f"{round(sucess_ratio * 100)}%"
