"""Explore data entries for a given dataset version using standard SQL."""
import json
from typing import List, Optional
import boto3
from uuid import UUID

from fastapi import APIRouter, Query
from fastapi.responses import ORJSONResponse
from ..datasets.queries import _get_geostore_geometry

from ...models.enum.geostore import GeostoreOrigin
from ...models.pydantic.responses import Response
from app.settings.globals import RASTER_ANALYSIS_LAMBDA_NAME

router = APIRouter()


@router.get(
    "/analysis",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Query"],
)
async def analysis(
    *,
    geostore_id: Optional[UUID] = Query(None, title="Geostore ID"),
    group_by: Optional[List[str]] = Query([], title="Group By"),
    filters: Optional[List[str]] = Query([], title="Filters"),
    sum: Optional[List[str]] = Query([], title="Sum"),
    start_date: Optional[str] =  Query(None, title="Start Date"),
    end_date: Optional[str] =  Query(None, title="End Date"),
    geostore_origin: GeostoreOrigin = Query(
        GeostoreOrigin.rw, title="Origin service of geostore ID"
    )
):
    geometry = await _get_geostore_geometry(geostore_id, geostore_origin)

    lambda_client = boto3.client("lambda")
    lambda_payload = {
        "geometry": geometry,
        "group_by": group_by,
        "filters": filters,
        "sum": sum,
        "start_date": start_date,
        "end_date": end_date
    }

    response = lambda_client.invoke(
        FunctionName=RASTER_ANALYSIS_LAMBDA_NAME,
        InvocationType="RequestResponse",
        Payload=bytes(json.dumps(lambda_payload), "utf-8"),
    )

    return Response(data=response)
