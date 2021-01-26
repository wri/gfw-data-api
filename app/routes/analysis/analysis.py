"""Run analysis on registered datasets."""
# import json
from typing import Any, Dict, List, Optional
from uuid import UUID

# import aioboto3
import boto3
import httpx
from fastapi import APIRouter, HTTPException, Path, Query
from fastapi.logger import logger
from fastapi.responses import ORJSONResponse
from httpx_auth import AWS4Auth

from ...models.enum.analysis import RasterLayer
from ...models.enum.geostore import GeostoreOrigin
from ...models.pydantic.analysis import ZonalAnalysisRequestIn
from ...models.pydantic.responses import Response
from ...settings.globals import (
    AWS_REGION,
    LAMBA_ENTRYPOINT_URL,
    RASTER_ANALYSIS_LAMBDA_NAME,
)
from ...utils.geostore import get_geostore_geometry

router = APIRouter()


@router.get(
    "/zonal/{geostore_id}",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Analysis"],
)
async def zonal_statistics_get(
    *,
    geostore_id: UUID = Path(..., title="Geostore ID"),
    geostore_origin: GeostoreOrigin = Query(
        GeostoreOrigin.gfw, title="Origin service of geostore ID"
    ),
    sum_layers: List[RasterLayer] = Query(..., alias="sum", title="Sum Layers"),
    group_by: List[RasterLayer] = Query([], title="Group By Layers"),
    filters: List[RasterLayer] = Query([], title="Filter Layers"),
    start_date: Optional[str] = Query(
        None,
        title="Start Date",
        description="Must be either year or YYYY-MM-DD date format.",
        regex="^\d{4}(\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01]))?$",
    ),
    end_date: Optional[str] = Query(
        None,
        title="End Date",
        description="Must be either year or YYYY-MM-DD date format.",
        regex="^\d{4}(\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01]))?$",
    ),
):
    """Calculate zonal statistics on any registered raster layers in a
    geostore."""
    geometry = await get_geostore_geometry(geostore_id, geostore_origin)
    return await _zonal_statics(
        geometry,
        sum_layers,
        group_by,
        filters,
        start_date,
        end_date,
    )


@router.post(
    "/zonal",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Analysis"],
)
async def zonal_statistics_post(request: ZonalAnalysisRequestIn):
    return await _zonal_statics(
        request.geometry,
        request.sum,
        request.group_by,
        request.filters,
        request.start_date,
        request.end_date,
    )


async def _zonal_statics(
    geometry: Dict[str, Any],
    sum_layers: List[RasterLayer],
    group_by: List[RasterLayer],
    filters: List[RasterLayer],
    start_date: Optional[str],
    end_date: Optional[str],
):
    payload = {
        "geometry": geometry,
        "group_by": group_by,
        "filters": filters,
        "sum": sum_layers,
        "start_date": start_date,
        "end_date": end_date,
    }

    response = await _invoke_lambda(payload)

    if response.status_code != 200:
        logger.error(
            f"Raster analysis lambda returned status code {response.status_code}"
        )
        raise HTTPException(
            500, "Raster analysis geoprocessor experienced an error. See logs."
        )

    response_data = response.json()["body"]["data"]
    return Response(data=response_data)


# async def _invoke_lambda(payload):
#     async with aioboto3.client("lambda", region_name=AWS_REGION) as lambda_client:
#         response = await lambda_client.invoke(
#             FunctionName=RASTER_ANALYSIS_LAMBDA_NAME,
#             InvocationType="RequestResponse",
#             Payload=bytes(json.dumps(payload), "utf-8"),
#         )
#
#         return await response["Payload"].read()


async def _invoke_lambda(payload, timeout=30) -> httpx.Response:
    session = boto3.Session()
    cred = session.get_credentials()

    aws = AWS4Auth(
        access_id=cred.access_key,
        secret_key=cred.secret_key,
        security_token=cred.token,
        region=AWS_REGION,
        service="lambda",
    )

    headers = {"X-Amz-Invocation-Type": "RequestResponse"}

    async with httpx.AsyncClient() as client:
        response: httpx.Response = await client.post(
            f"{LAMBA_ENTRYPOINT_URL}/2015-03-31/functions/{RASTER_ANALYSIS_LAMBDA_NAME}/invocations",
            json=payload,
            auth=aws,
            timeout=timeout,
            headers=headers,
        )

    print(f"Lambda response: {response}")

    return response
