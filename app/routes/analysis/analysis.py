"""Run analysis on registered datasets."""
from io import StringIO
from json.decoder import JSONDecodeError
from typing import List, Optional, Dict, Any
from uuid import UUID

import boto3
import httpx
from fastapi import APIRouter, HTTPException, Path, Query
from fastapi.encoders import jsonable_encoder
from fastapi.logger import logger
from fastapi.responses import ORJSONResponse
from httpx_auth import AWS4Auth

from ...models.enum.analysis import RasterLayer
from ...models.enum.geostore import GeostoreOrigin
from ...models.pydantic.analysis import ZonalAnalysisRequestIn, RasterQueryRequestIn
from ...models.pydantic.geostore import Geometry
from ...models.pydantic.responses import Response
from ...responses import CSVStreamingResponse
from ...settings.globals import (
    AWS_REGION,
    LAMBDA_ENTRYPOINT_URL,
    RASTER_ANALYSIS_LAMBDA_NAME,
)
from ...utils.geostore import get_geostore_geometry
from .. import DATE_REGEX

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
        regex=DATE_REGEX,
    ),
    end_date: Optional[str] = Query(
        None,
        title="End Date",
        description="Must be either year or YYYY-MM-DD date format.",
        regex=DATE_REGEX,
    ),
):
    """Calculate zonal statistics on any registered raster layers in a
    geostore."""
    geometry: Geometry = await get_geostore_geometry(geostore_id, geostore_origin)
    return await _zonal_statistics(
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
    return await _zonal_statistics(
        request.geometry,
        request.sum,
        request.group_by,
        request.filters,
        request.start_date,
        request.end_date,
    )


@router.get(
    "/raster/query",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Analysis"],
)
async def raster_query_get(
    *,
    geostore_id: UUID = Path(..., title="Geostore ID"),
    geostore_origin: GeostoreOrigin = Query(
        GeostoreOrigin.gfw, title="Origin service of geostore ID"
    ),
    sql: str = Query(..., title="Query")
):
    geometry: Geometry = await get_geostore_geometry(geostore_id, geostore_origin)
    return await _raster_query(
        geometry,
        sql
    )


@router.get(
    "/raster/download",
    response_class=ORJSONResponse,
    response_model=CSVStreamingResponse,
    tags=["Analysis"],
)
async def raster_download_get(
    *,
    geostore_id: UUID = Path(..., title="Geostore ID"),
    geostore_origin: GeostoreOrigin = Query(
        GeostoreOrigin.gfw, title="Origin service of geostore ID"
    ),
    sql: str = Query(..., title="Query"),
    filename: str = Query("export.csv", description="Name of export file."),
):
    geometry: Geometry = await get_geostore_geometry(geostore_id, geostore_origin)
    data = await _raster_query(
        geometry,
        sql,
        format="csv"
    )

    return CSVStreamingResponse(iter([data]), filename=filename)


@router.post(
    "/raster/query",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Analysis"],
)
async def raster_query_post(request: RasterQueryRequestIn):
    return await _raster_query(
        request.geometry,
        request.sql
    )


async def _zonal_statistics(
        geometry: Dict[str, Any],
        sum_layers: List[RasterLayer],
        group_by: List[RasterLayer],
        filters: List[RasterLayer],
        start_date: Optional[str],
        end_date: Optional[str],
):
    selectors = ",".join([f"sum({lyr.value})" for lyr in sum_layers])
    groups = ",".join([lyr.value for lyr in group_by])

    where_clauses = []
    for lyr in filters:
        if "umd_tree_cover_density" in lyr.value:
            where_clauses.append(f"{lyr.value[:-2]}__threshold = {lyr.value[-2:]}")
        else:
            where_clauses.append(f"{lyr.value} != 0")

    if start_date:
        where_clauses.append(_get_date_filter(start_date, ">="))

    if end_date:
        where_clauses.append(_get_date_filter(end_date, "<"))

    where = " and ".join(filters)

    query = f"select {selectors} from data"
    if where:
        query += f" where {where}"
    query += f"group by {groups}"

    return await _raster_query(
        geometry,
        query
    )


def _get_date_filter(date: str, op: str):
    if len(date) == 4:
        return f"umd_tree_cover_loss__year {op} {date}"
    else:
        return f"umd_glad_alerts__date {op} {date}"


async def _raster_query(
    geometry: Geometry,
    query: str,
    format: Optional[str] = "json"
):
    payload = {
        "geometry": jsonable_encoder(geometry),
        "query": query,
        "format": format
    }

    try:
        response = await _invoke_lambda(payload)
    except httpx.TimeoutException:
        raise HTTPException(500, "Query took too long to process.")

    try:
        response_data = response.json()["body"]["data"]
    except (JSONDecodeError, KeyError):
        logger.error(
            f"Raster analysis lambda experienced an error. Full response: {response.text}"
        )
        raise HTTPException(
            500, "Raster analysis geoprocessor experienced an error. See logs."
        )

    return Response(data=response_data)


async def _invoke_lambda(payload, timeout=55) -> httpx.Response:
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
            f"{LAMBDA_ENTRYPOINT_URL}/2015-03-31/functions/{RASTER_ANALYSIS_LAMBDA_NAME}/invocations",
            json=payload,
            auth=aws,
            timeout=timeout,
            headers=headers,
        )

    return response
